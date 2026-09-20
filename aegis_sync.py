import os
import json
import requests
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

def main():
    print("[AEGIS Node] Initializing USASpending Federal Contract extraction...")
    
    # 1. BigQuery Setup
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    
    # We rename the table to reflect the new intelligence stream
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.aegis_procurement"
    
    # 2. USASpending API Setup (No Auth Required)
    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    
    # Target prime contractors (can be expanded to match master_tickers)
    target_recipients = [
        "LOCKHEED MARTIN CORPORATION", 
        "BOEING COMPANY, THE", 
        "GENERAL DYNAMICS CORPORATION", 
        "NORTHROP GRUMMAN CORPORATION", 
        "PALANTIR TECHNOLOGIES INC.",
        "MICROSOFT CORPORATION",
        "AMAZON WEB SERVICES, INC."
    ]
    
    recent_date = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')
    today_date = datetime.utcnow().strftime('%Y-%m-%d')
    timestamp_iso = datetime.utcnow().isoformat()
    bq_payload = []
    
    # 3. Extraction Loop
    for recipient in target_recipients:
        try:
            print(f"[AEGIS] Scanning USASpending for {recipient}...")
            
            # USASpending API POST Payload
            payload = {
                "filters": {
                    "time_period": [{"start_date": recent_date, "end_date": today_date}],
                    "award_type_codes": ["A", "B", "C", "D"], # Contract award types
                    "recipient_search_text": [recipient]
                },
                "fields": [
                    "Award ID", "Recipient Name", "Awarding Agency", 
                    "Award Amount", "Start Date", "Description"
                ],
                "limit": 50,
                "page": 1
            }
            
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                
                if results:
                    for award in results:
                        amount = award.get("Award Amount", 0)
                        # Filter out micro-purchases; focus on contracts > $100k
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
                                "description": str(award.get("Description", "N/A"))[:200] # Truncate long descriptions
                            })
            else:
                print(f"[AEGIS ERROR] API rejected request: {response.status_code}")
        except Exception as e:
            print(f"[AEGIS ERROR] Failed to fetch contracts for {recipient}: {e}")

    # 4. BigQuery Ingestion
    if bq_payload:
        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=True, # Automatically creates the flat aegis_procurement table
            )
            job = client.load_table_from_json(bq_payload, table_id, job_config=job_config)
            job.result()  
            print(f"[AEGIS] Successfully loaded {len(bq_payload)} federal contracts into BigQuery.")
        except Exception as e:
            print(f"[AEGIS ERROR] BigQuery push failed: {e}")
    else:
        print("[AEGIS] No new major contracts found in this cycle.")

if __name__ == "__main__":
    main()
    
