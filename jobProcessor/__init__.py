"""
Job Processor - Orchestrates the schema extraction workflow
Reads from: {container}/{user_id}/{job_id}/*.parquet
Writes to: {container}/{user_id}/{job_id}/metadata/schema_*.json
"""
import azure.functions as func
import json
import logging
import io
import sys
import os
from datetime import datetime

# Add root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

import pandas as pd
from azure.storage.blob import BlobServiceClient
from shared.shared import (
    extract_schema_metadata,
    extract_schema_from_json_file,
    SUPPORTED_FORMATS,
    STORAGE_ACCOUNT_NAME,
    STORAGE_ACCOUNT_KEY,
    NumpyEncoder
)

def main(myblob: func.InputStream):
    """
    Triggered when a job_status.json file is created.
    Processes all files in the user's job folder.
    
    Trigger path: {container}/{user_id}/{job_id}/metadata/job_status.json
    """
    try:
        logging.info("=" * 60)
        logging.info("🚀 Job Processor Started")
        logging.info("=" * 60)
        
        # Extract paths from blob trigger
        # myblob.name format: "datamodelling/user123/job456/metadata/job_status.json"
        blob_path_parts = myblob.name.split('/')
        
        if len(blob_path_parts) < 4:
            logging.error(f"❌ Invalid blob path: {myblob.name}")
            return
        
        container_name = blob_path_parts[0]
        user_id = blob_path_parts[1]
        job_id = blob_path_parts[2]
        
        logging.info(f"📦 Container: {container_name}")
        logging.info(f"👤 User ID: {user_id}")
        logging.info(f"📋 Job ID: {job_id}")
        
        # Load job info
        job_data = json.loads(myblob.read().decode())
        current_status = job_data.get('status', 'unknown')
        
        logging.info(f"📊 Current Status: {current_status}")
        
        # ✅ CRITICAL: Prevent re-processing
        if current_status != 'initiated':
            logging.info(f"⏩ Job {job_id} already processed (status: {current_status}). Skipping.")
            return
        
        # Connect to storage
        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={STORAGE_ACCOUNT_NAME};"
            f"AccountKey={STORAGE_ACCOUNT_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
        
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service.get_container_client(container_name)
        
        # Update job status to processing immediately
        job_data['status'] = 'processing'
        job_data['started_at'] = datetime.utcnow().isoformat()
        
        job_status_path = f"{user_id}/{job_id}/metadata/job_status.json"
        job_status_blob = container_client.get_blob_client(job_status_path)
        job_status_blob.upload_blob(
            json.dumps(job_data, indent=2),
            overwrite=True
        )
        
        logging.info(f"✅ Job status updated to 'processing'")
        
        # Define paths
        job_folder = f"{user_id}/{job_id}/"
        metadata_folder = f"{job_folder}metadata/"
        relationships_folder = f"{job_folder}relationships/"
        
        processed = []
        failed = []
        
        # Process all supported files in job folder
        logging.info(f"🔍 Scanning job folder: {job_folder}")
        
        for blob in container_client.list_blobs(name_starts_with=job_folder):
            file_path = blob.name
            file_name = file_path.split('/')[-1]
            file_ext = '.' + file_name.split('.')[-1].lower() if '.' in file_name else ''
            
            # Skip metadata and relationships folders
            if file_path.startswith(metadata_folder) or file_path.startswith(relationships_folder):
                continue
            
            # Skip unsupported file types
            if file_ext not in SUPPORTED_FORMATS:
                logging.info(f"⏩ Skipping unsupported file: {file_name}")
                continue
            
            try:
                logging.info(f"📄 Processing: {file_name}")
                
                # Download file
                blob_client = container_client.get_blob_client(file_path)
                data = blob_client.download_blob().readall()
                
                # Extract schema based on file type
                metadata = None
                
                if file_ext == ".json":
                    metadata = extract_schema_from_json_file(data, file_name, file_path)
                elif file_ext == ".csv":
                    df = pd.read_csv(io.BytesIO(data))
                    metadata = extract_schema_metadata(df, file_name, file_path)
                elif file_ext == ".parquet":
                    df = pd.read_parquet(io.BytesIO(data))
                    metadata = extract_schema_metadata(df, file_name, file_path)
                
                if metadata:
                    # Add additional metadata
                    metadata["blob_size_bytes"] = blob.size
                    metadata["user_id"] = user_id
                    metadata["job_id"] = job_id
                    
                    # Save schema to metadata folder
                    schema_filename = f"schema_{file_name}.json"
                    schema_path = f"{metadata_folder}{schema_filename}"
                    
                    schema_blob = container_client.get_blob_client(schema_path)
                    schema_blob.upload_blob(
                        json.dumps(metadata, indent=2, cls=NumpyEncoder),
                        overwrite=True
                    )
                    
                    processed.append(file_name)
                    logging.info(f"✅ Saved: {schema_path}")
                else:
                    logging.warning(f"⚠️ No metadata extracted from: {file_name}")
            
            except Exception as e:
                logging.error(f"❌ Failed to process {file_name}: {e}")
                failed.append({"file": file_name, "error": str(e)})
        
        # Update job with results
        job_data['status'] = 'schema_extraction_complete'
        job_data['processed_files'] = processed
        job_data['failed_files'] = failed
        job_data['schema_completed_at'] = datetime.utcnow().isoformat()
        
        job_status_blob.upload_blob(
            json.dumps(job_data, indent=2, cls=NumpyEncoder),
            overwrite=True
        )
        
        logging.info("=" * 60)
        logging.info(f"✅ Job Processor Complete")
        logging.info(f"   User: {user_id}")
        logging.info(f"   Job: {job_id}")
        logging.info(f"   Processed: {len(processed)} files")
        logging.info(f"   Failed: {len(failed)} files")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.exception(f"🔥 Job Processor failed: {e}")
        
        # Update job status to failed
        try:
            # Extract info from blob path
            blob_path_parts = myblob.name.split('/')
            if len(blob_path_parts) >= 4:
                container_name = blob_path_parts[0]
                user_id = blob_path_parts[1]
                job_id = blob_path_parts[2]
                
                conn_str = (
                    f"DefaultEndpointsProtocol=https;"
                    f"AccountName={STORAGE_ACCOUNT_NAME};"
                    f"AccountKey={STORAGE_ACCOUNT_KEY};"
                    f"EndpointSuffix=core.windows.net"
                )
                
                blob_service = BlobServiceClient.from_connection_string(conn_str)
                container_client = blob_service.get_container_client(container_name)
                
                # Read current job data
                job_status_path = f"{user_id}/{job_id}/metadata/job_status.json"
                job_status_blob = container_client.get_blob_client(job_status_path)
                
                try:
                    job_data = json.loads(job_status_blob.download_blob().readall().decode())
                except:
                    job_data = {"user_id": user_id, "job_id": job_id}
                
                job_data['status'] = 'failed'
                job_data['error'] = str(e)
                job_data['failed_at'] = datetime.utcnow().isoformat()
                
                job_status_blob.upload_blob(
                    json.dumps(job_data, indent=2),
                    overwrite=True
                )
                
                logging.info(f"✅ Updated job status to 'failed'")
        except Exception as cleanup_error:
            logging.error(f"Failed to update job status to failed: {cleanup_error}")
        
        raise