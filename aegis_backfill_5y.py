import os
import json
import time
import requests
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

def main():
    print("[AEGIS BACKFILL] Initializing 5-Year Federal Contract Extraction...")
    
    # 1. BigQuery Setup
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.aegis_procurement"
    
    # 2. Timeframes & Targets (5 Years)
    end_date = datetime.utcnow().strftime('%Y-%m-%d')
    start_date = (datetime.utcnow() - timedelta(days=5*365)).strftime('%Y-%m-%d')
    timestamp_iso = datetime.utcnow().isoformat()
    
    target_recipients = [
        "LOCKHEED MARTIN CORPORATION", "BOEING COMPANY, THE", 
        "GENERAL DYNAMICS CORPORATION", "NORTHROP GRUMMAN CORPORATION", 
        "PALANTIR TECHNOLOGIES INC.", "MICROSOFT CORPORATION", "AMAZON WEB SERVICES, INC."
    ]
    
    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    bq_payload = []
    
    # 3. Extraction Loop with Pagination
    for recipient in target_recipients:
        print(f"[AEGIS] Scanning 5-year history for {recipient}...")
        page = 1
        has_more_data = True
        
        while has_more_data:
            payload = {
                "filters": {
                    "time_period": [{"start_date": start_date, "end_date": end_date}],
                    "award_type_codes": ["A", "B", "C", "D"], # Core contract types
                    "recipient_search_text": [recipient]
                },
                "fields": [
                    "Award ID", "Recipient Name", "Awarding Agency", 
                    "Award Amount", "Start Date", "Description"
                ],
                "limit": 100,
                "page": page
            }
            
            try:
                response = requests.post(url, json=payload)
                if response.status_code == 200:
                    data = response.json()
                    results = data.get("results", [])
                    
                    if not results:
                        has_more_data = False
                        break
                        
                    for award in results:
                        amount = award.get("Award Amount", 0)
                        # Filter out micro-purchases
                        if amount and amount > 100000:
                            bq_payload.append({
                                "timestamp": timestamp_iso,
                                "domain": "AEGIS",
                                "entity_id": recipient,
                                "signal_type": "Federal Contract Award",
                                "award_id": award.get("Award ID", "UNKNOWN"),
                                "awarding_agency": award.get("Awarding Agency", "N/A"),
                                "award_amount": float(amount),
                                "date_signed": award.get("Start Date", "N/A"),
                                "description": str(award.get("Description", "N/A"))[:200]
                            })
                    
                    # Check if there are more pages
                    if data.get("page_metadata", {}).get("hasNext", False):
                        page += 1
                        time.sleep(0.5) # Throttle to respect public API
                    else:
                        has_more_data = False
                else:
                    print(f"[AEGIS ERROR] API rejected request on page {page}: {response.status_code}")
                    has_more_data = False
            except Exception as e:
                print(f"[AEGIS ERROR] Failed on {recipient} page {page}: {e}")
                has_more_data = False

    # 4. BigQuery Ingestion
    total_records = len(bq_payload)
    print(f"[AEGIS BACKFILL] Extracted {total_records} historical contracts. Ingesting...")
    
    if total_records > 0:
        chunk_size = 10000
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True
        )
        
        for idx in range(0, total_records, chunk_size):
            chunk = bq_payload[idx:idx + chunk_size]
            try:
                job = client.load_table_from_json(chunk, table_id, job_config=job_config)
                job.result()
                print(f"[AEGIS] Committed chunk {idx+1} to {min(idx + chunk_size, total_records)}.")
            except Exception as e:
                print(f"[AEGIS ERROR] BigQuery push failed: {e}")
    else:
        print("[AEGIS] No records found.")

if __name__ == "__main__":
    main()
  
