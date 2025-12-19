"""
AI Analyzer - Detects relationships between tables
Reads from: {container}/{user_id}/{job_id}/metadata/schema_*.json
Writes to: {container}/{user_id}/{job_id}/relationships/relationship.json
"""
import azure.functions as func
import json
import logging
from datetime import datetime
import re
import sys
import os

# Add root directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared.shared import (
    detect_relationships,
    NumpyEncoder,
    STORAGE_ACCOUNT_NAME,
    STORAGE_ACCOUNT_KEY
)

def main(myblob: func.InputStream):
    """
    Triggered when a schema file is added to the metadata folder.
    Processes all schemas for the job and detects relationships.
    
    Trigger path: {container}/{user_id}/{job_id}/metadata/schema_{name}.json
    """
    try:
        logging.info("=" * 60)
        logging.info("🧠 AIAnalyzer Started")
        logging.info("=" * 60)
        
        # Extract paths from blob trigger
        # myblob.name format: "datamodelling/user123/job456/metadata/schema_customers.parquet.json"
        blob_path = myblob.name
        logging.info(f"📥 Triggered by blob: {blob_path}")
        
        blob_path_parts = blob_path.split('/')
        
        if len(blob_path_parts) < 5:
            logging.error(f"❌ Invalid blob path: {blob_path}")
            return
        
        container_name = blob_path_parts[0]
        user_id = blob_path_parts[1]
        job_id = blob_path_parts[2]
        # blob_path_parts[3] is "metadata"
        schema_filename = blob_path_parts[4]
        
        logging.info(f"📦 Container: {container_name}")
        logging.info(f"👤 User ID: {user_id}")
        logging.info(f"📋 Job ID: {job_id}")
        logging.info(f"📄 Schema File: {schema_filename}")
        
        # ⚠️ CRITICAL: Only process valid schema files
        if schema_filename.count('schema_') > 1:
            logging.info(f"✅ Ignoring malformed recursive file: {schema_filename}")
            return
        
        if not schema_filename.startswith("schema_"):
            logging.info(f"✅ Skipping non-schema file: {schema_filename}")
            return
        
        # Connect to storage
        from azure.storage.blob import BlobServiceClient
        
        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={STORAGE_ACCOUNT_NAME};"
            f"AccountKey={STORAGE_ACCOUNT_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
        
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service.get_container_client(container_name)
        
        # Check job status
        job_status_path = f"{user_id}/{job_id}/metadata/job_status.json"
        
        try:
            job_status_blob = container_client.get_blob_client(job_status_path)
            job_data = json.loads(job_status_blob.download_blob().readall().decode())
        except Exception as e:
            logging.error(f"❌ Job status file not found: {job_status_path}")
            return
        
        current_status = job_data.get('status', 'unknown')
        logging.info(f"📊 Job Status: {current_status}")
        
        # Only process if schemas are complete
        if current_status == 'completed':
            logging.info(f"✅ Job {job_id} already completed. Skipping.")
            return
        
        if current_status != 'schema_extraction_complete':
            logging.info(f"⏩ Job {job_id} not ready for analysis (status: {current_status})")
            return
        
        # Load all schemas for this job
        logging.info("📋 Step 1: Loading all schemas for this job...")
        
        metadata_folder = f"{user_id}/{job_id}/metadata/"
        schemas = []
        
        for blob in container_client.list_blobs(name_starts_with=metadata_folder):
            blob_name = blob.name.split('/')[-1]
            
            # Only load schema files
            if not blob_name.startswith("schema_") or blob_name == "job_status.json":
                continue
            
            # Skip malformed files
            if blob_name.count('schema_') > 1:
                logging.warning(f"⚠️ Skipping malformed file: {blob_name}")
                continue
            
            try:
                schema_blob = container_client.get_blob_client(blob.name)
                schema = json.loads(schema_blob.download_blob().readall().decode())
                
                # Verify it belongs to this job
                if schema.get('user_id') == user_id and schema.get('job_id') == job_id:
                    schemas.append(schema)
                    table_name = schema.get("table_name", "unknown")
                    num_cols = len(schema.get("columns", []))
                    logging.info(f"   ✓ Loaded: {table_name} ({num_cols} columns)")
            except Exception as e:
                logging.error(f"   ✗ Failed to load {blob_name}: {e}")
        
        if not schemas:
            logging.error(f"❌ No valid schemas found for job {job_id}")
            return
        
        logging.info(f"✅ Successfully loaded {len(schemas)} schemas")
        
        # Detect relationships
        logging.info("📋 Step 2: Detecting relationships with AI...")
        relationship_info = detect_relationships(schemas)
        
        num_rels = len(relationship_info.get('relationships', []))
        num_tables = len(relationship_info.get('tables', []))
        
        logging.info(f"✅ Relationship detection complete:")
        logging.info(f"   - Tables: {num_tables}")
        logging.info(f"   - Relationships: {num_rels}")
        
        # Log relationships
        if num_rels > 0:
            logging.info("🔗 Detected relationships:")
            for rel in relationship_info['relationships'][:5]:
                from_t = rel.get('from_table', '')
                to_t = rel.get('to_table', '')
                rel_type = rel.get('relationship_type', '')
                logging.info(f"   {from_t} -> {to_t} ({rel_type})")
        else:
            logging.warning("⚠️ No relationships detected")
        
        # Build enriched output with full schema details
        logging.info("📋 Step 3: Preparing enriched output...")
        
        enriched_tables = []
        for schema in schemas:
            table_name = schema.get("table_name", "")
            columns = schema.get("columns", [])
            row_count = schema.get("row_count", 0)
            
            # Find primary and foreign keys
            table_info = next(
                (t for t in relationship_info.get("tables", []) if t["table"] == table_name), 
                None
            )
            primary_keys = table_info.get("primary_keys", []) if table_info else []
            foreign_keys = table_info.get("foreign_keys", []) if table_info else []
            
            # Determine table type
            is_fact = table_name in relationship_info.get("fact_tables", [])
            is_dimension = table_name in relationship_info.get("dimension_tables", [])
            table_type = "FACT" if is_fact else ("DIM" if is_dimension else "TABLE")
            
            enriched_tables.append({
                "table_name": table_name,
                "table_type": table_type,
                "row_count": row_count,
                "column_count": len(columns),
                "null_percentage": round(
                    sum(col.get("null_percentage", 0) for col in columns) / len(columns), 1
                ) if columns else 0,
                "primary_keys": primary_keys,
                "foreign_keys": foreign_keys,
                "columns": [
                    {
                        "name": col.get("column_name", ""),
                        "data_type": col.get("data_type", ""),
                        "null_percentage": round(col.get("null_percentage", 0), 1),
                        "is_primary_key": col.get("column_name") in primary_keys,
                        "is_foreign_key": any(
                            fk.get("column") == col.get("column_name") for fk in foreign_keys
                        )
                    }
                    for col in columns
                ]
            })
        
        # Build enriched relationships with actual column names
        enriched_relationships = []
        for rel in relationship_info.get("relationships", []):
            from_table = rel.get("from_table", "")
            to_table = rel.get("to_table", "")
            
            # Find the foreign key details
            from_table_info = next(
                (t for t in relationship_info.get("tables", []) if t["table"] == from_table),
                None
            )
            if from_table_info:
                for fk in from_table_info.get("foreign_keys", []):
                    if fk.get("references_table") == to_table:
                        enriched_relationships.append({
                            "from_table": from_table,
                            "from_column": fk.get("column", ""),
                            "to_table": to_table,
                            "to_column": fk.get("references_column", ""),
                            "relationship_type": rel.get("relationship_type", "M:1"),
                            "cardinality": rel.get("cardinality", f"Many {from_table} to One {to_table}")
                        })
                        break
        
        # Create final output
        complete_analysis = {
            "analysis_timestamp": datetime.utcnow().isoformat(),
            "user_id": user_id,
            "job_id": job_id,
            "schemas_analyzed": len(schemas),
            "tables": enriched_tables,
            "relationships": enriched_relationships,
            "summary": {
                "total_tables": len(enriched_tables),
                "fact_tables": relationship_info.get("fact_tables", []),
                "dimension_tables": relationship_info.get("dimension_tables", []),
                "total_relationships": len(enriched_relationships),
                "total_rows": sum(t["row_count"] for t in enriched_tables)
            }
        }
        
        # Save relationship file
        logging.info("📋 Step 4: Saving relationship analysis...")
        
        relationship_path = f"{user_id}/{job_id}/relationships/relationship.json"
        relationship_blob = container_client.get_blob_client(relationship_path)
        
        relationship_blob.upload_blob(
            json.dumps(complete_analysis, indent=2, cls=NumpyEncoder),
            overwrite=True
        )
        
        logging.info(f"✅ Saved relationship to: {relationship_path}")
        
        # Update job status to completed
        job_data['status'] = 'completed'
        job_data['completed_at'] = datetime.utcnow().isoformat()
        job_data['relationship_file'] = relationship_path
        
        job_status_blob.upload_blob(
            json.dumps(job_data, indent=2, cls=NumpyEncoder),
            overwrite=True
        )
        
        logging.info("=" * 60)
        logging.info(f"🎉 AIAnalyzer completed successfully!")
        logging.info(f"   User: {user_id}")
        logging.info(f"   Job: {job_id}")
        logging.info(f"   Tables: {num_tables}")
        logging.info(f"   Relationships: {num_rels}")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.exception(f"🔥 AIAnalyzer failed: {e}")
        
        # Update job status to failed
        try:
            blob_path_parts = myblob.name.split('/')
            if len(blob_path_parts) >= 5:
                container_name = blob_path_parts[0]
                user_id = blob_path_parts[1]
                job_id = blob_path_parts[2]
                
                from azure.storage.blob import BlobServiceClient
                
                conn_str = (
                    f"DefaultEndpointsProtocol=https;"
                    f"AccountName={STORAGE_ACCOUNT_NAME};"
                    f"AccountKey={STORAGE_ACCOUNT_KEY};"
                    f"EndpointSuffix=core.windows.net"
                )
                
                blob_service = BlobServiceClient.from_connection_string(conn_str)
                container_client = blob_service.get_container_client(container_name)
                
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
            logging.error(f"Failed to update job status: {cleanup_error}")
        
        raise