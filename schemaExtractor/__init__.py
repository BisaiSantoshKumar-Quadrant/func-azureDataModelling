"""
schemaExtractor Function
"""
import logging
import azure.functions as func
import json
import sys
import os
import io

from datetime import datetime

import logging

import uuid
from azure.storage.blob import BlobServiceClient

logging.info("🔍 BEFORE IMPORT SHARED")
try:
    from ..shared import shared
    logging.info("✅ SHARED IMPORTED SUCCESSFULLY")
except Exception as e:
    logging.error(f"🔥 SHARED IMPORT FAILED: {e}")
    raise



# Add root directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

# Import shared modules
import pandas as pd
from shared.shared import (
    extract_schema_metadata,
    save_to_blob,
    read_blob_if_exists,
    SUPPORTED_FORMATS,
    METADATA_CONTAINER,
    BATCH_INFO_BLOB,
    NumpyEncoder,
    extract_schema_from_json_file
)

def main(myblob: func.InputStream):
    try:
        logging.info(f"📄 Processing blob: {myblob.name}")
        blob_name = myblob.name.split('/')[-1]
        file_name = myblob.name.split('/')[-1]
        file_ext = '.' + file_name.split('.')[-1].lower()

        # Skip job marker files
        if blob_name.startswith("job_"):
            logging.info(f"⏩ Skipping job marker: {blob_name}")
            return

        # Skip ALL internal/generated files
        if blob_name.startswith("schema_") or \
           blob_name.startswith("analysis_") or \
           blob_name.startswith("relationship") or \
           blob_name.startswith("ddl_") or \
           blob_name == "batch_info.json" or \
           "metadata/" in myblob.name or \
           "ddl-scripts/" in myblob.name or \
           "relationships/" in myblob.name:
            logging.info(f"⏩ Skipping internal file: {myblob.name}")
            return
        
        # Validate type
        if file_ext not in SUPPORTED_FORMATS:
            logging.warning(f"❌ Unsupported file type: {file_ext}")
            return

        # Load batch info
        batch_info = read_blob_if_exists(BATCH_INFO_BLOB)

        if batch_info is None:
            batch_info = {
                "batch_start": datetime.utcnow().isoformat(),
                "batch_id": datetime.utcnow().strftime('%Y%m%d_%H%M%S'),
                "processed_files": [],
                "failed_files": []
            }
        else:
            if "batch_id" not in batch_info:
                batch_info["batch_id"] = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

        # Dedup check
        if any(f["file_path"] == myblob.name for f in batch_info["processed_files"]):
            logging.info(f"⏩ File already processed: {myblob.name}")
            return

        # Read file data
        logging.info(f"📖 Reading file {file_name}")
        data = myblob.read()

        # Schema Extraction Logic
        if file_ext == ".json":
            metadata = extract_schema_from_json_file(data, file_name, myblob.name)
            if metadata is None:
                logging.error(f"❌ Failed to extract JSON schema from {file_name}")
                return
        
        elif file_ext == ".csv":
            try:
                df = pd.read_csv(io.BytesIO(data))
            except:
                df = pd.read_csv(io.BytesIO(data), engine="python", on_bad_lines="skip")
            
            if df is None or df.empty:
                logging.warning(f"⚠ Empty CSV file")
                return
            metadata = extract_schema_metadata(df, file_name, myblob.name)
        
        elif file_ext == ".parquet":
            df = pd.read_parquet(io.BytesIO(data))
            if df is None or df.empty:
                logging.warning(f"⚠ Empty Parquet file")
                return
            metadata = extract_schema_metadata(df, file_name, myblob.name)

        # Add blob size
        metadata["blob_size_bytes"] = myblob.length

        # Save result
        ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        schema_file = f"schema_{file_name}_{ts}.json"

        save_to_blob(
            json.dumps(metadata, indent=2, cls=NumpyEncoder),
            schema_file,
            METADATA_CONTAINER
        )

        logging.info(f"✅ Schema saved: {schema_file}")

        # Update batch info
        batch_info["processed_files"].append({
            "file_path": myblob.name,
            "file_name": file_name,
            "file_extension": file_ext,
            "timestamp": datetime.utcnow().isoformat()
        })

        save_to_blob(
            json.dumps(batch_info, indent=2, cls=NumpyEncoder),
            BATCH_INFO_BLOB,
            METADATA_CONTAINER
        )

        logging.info(f"✅ Processing complete for {file_name}")

    except Exception as e:
        logging.exception(f"🔥 CRITICAL FAILURE in main: {e}")
        raise

