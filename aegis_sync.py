import os
import json
import requests
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

def main():
    print("[AEGIS Node] Initializing USPTO Intellectual Property extraction...")
    
    # BigQuery Setup
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    
    # Dedicated flat table for AEGIS
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.aegis_ip"
    
    # Target entities: We can expand this list later to match your master_tickers
    target_orgs = [
        "Apple Inc.", "Microsoft Corporation", "Google LLC", 
        "Amazon Technologies, Inc.", "Nvidia Corporation", 
        "Meta Platforms, Inc.", "Tesla, Inc."
    ]
    
    timestamp_iso = datetime.utcnow().isoformat()
    bq_payload = []
    
    # The USPTO issues patents on Tuesdays. We look back 30 days to catch recent grants.
    recent_date = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')

    for org in target_orgs:
        try:
            print(f"[AEGIS] Scanning USPTO for {org}...")
            url = "https://api.patentsview.org/patents/query"
            
            # PatentsView Query DSL
            query = {
                "_and": [
                    {"_contains": {"assignee_organization": org}},
                    {"_gte": {"patent_date": recent_date}}
                ]
            }
            fields = ["patent_number", "patent_title", "patent_date", "assignee_organization"]
            payload = {"q": query, "f": fields, "o": {"per_page": 25}}
            
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                data = response.json()
                patents = data.get("patents", [])
                
                if patents:
                    for p in patents:
                        assignees = p.get("assignees", [{}])
                        org_name = assignees[0].get("assignee_organization", org) if assignees else org
                        
                        bq_payload.append({
                            "timestamp": timestamp_iso,
                            "domain": "AEGIS",
                            "entity_id": org_name,
                            "signal_type": "USPTO Patent Grant",
                            "patent_id": p.get("patent_number", "UNKNOWN"),
                            "patent_title": p.get("patent_title", "N/A"),
                            "filing_date": p.get("patent_date", "N/A")
                        })
        except Exception as e:
            print(f"[AEGIS ERROR] Failed to fetch IP for {org}: {e}")

    if bq_payload:
        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=True, # Automatically creates the flat aegis_ip table
            )
            job = client.load_table_from_json(bq_payload, table_id, job_config=job_config)
            job.result()  
            print(f"[AEGIS] Successfully loaded {len(bq_payload)} patent records into BigQuery.")
        except Exception as e:
            print(f"[AEGIS ERROR] BigQuery push failed: {e}")
    else:
        print("[AEGIS] No new patents found in this cycle.")

if __name__ == "__main__":
    main()
  
