import io
import json
import re
import oci
import logging
from fdk import response

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Hardcoded Topic OCID
TOPIC_OCID = "<ONS-TOPIC-OCID>"

def process_entry(entry, client):
    try:
        db_name = entry.get("source", "Unknown DB")
        alert_log_path = entry.get("subject", "No path")
        timestamp = entry.get("time", "N/A")
        error_message = entry.get("data", {}).get("message", "No error details")
        tailed_path = entry.get("data", {}).get("tailed_path", alert_log_path)

        # Extract ORA-XXXX using regex
        match = re.search(r"(ORA-\d+)", error_message or "")
        ora_code = match.group(1) if match else None

        # Build subject dynamically
        subject = f"{ora_code if ora_code else 'Error'} detected in {db_name}"
        body_text = f"""
Database Name  : {db_name}
Detected Error : {ora_code if ora_code else 'No ORA code found'}
Full Message   : {error_message}
Alert File     : {tailed_path}
Timestamp      : {timestamp}
"""

        logger.info(f"Publishing message for DB: {db_name}, Error: {ora_code if ora_code else 'None'}")
        msg = oci.ons.models.MessageDetails(body=body_text, title=subject)
        client.publish_message(TOPIC_OCID, msg)

    except Exception as e:
        logger.error(f"Failed to process entry for {entry.get('source', 'Unknown')}: {str(e)}")

def handler(ctx, data: io.BytesIO = None):
    try:
        raw_body = data.getvalue()
        if not raw_body:
            raise ValueError("Empty payload received")

        body = json.loads(raw_body)
        signer = oci.auth.signers.get_resource_principals_signer()
        client = oci.ons.NotificationDataPlaneClient(config={}, signer=signer)

        logger.info(f"Received payload: {body}")

        if isinstance(body, list) and len(body) > 0:
            for entry in body:
                process_entry(entry, client)
        elif isinstance(body, dict):
            process_entry(body, client)
        else:
            raise ValueError("Invalid payload format")

        return response.Response(ctx, response_data="Notifications sent successfully", headers={"Content-Type": "text/plain"})

    except Exception as ex:
        logger.error(f"Function error: {str(ex)}")
        return response.Response(ctx, response_data=f"Error: {str(ex)}", headers={"Content-Type": "text/plain"})
