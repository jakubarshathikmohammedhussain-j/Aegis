import os
import json
import time
import requests
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

def main():
    print("[AEGIS TIER 1] Initializing 10-Year Federal Contract Ingestion for Tier 1 Primes...")

    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.aegis_procurement"

    # 1. Fetch company names from the master dimension
    query = f"""
        SELECT DISTINCT ticker, company_name, gics_sector 
        FROM `{creds_dict['project_id']}.telemetry_bronze.tier1_master_index`
        WHERE gics_sector IN ('Industrials', 'Information Technology', 'Health Care', 'Energy')
    """
    targets = client.query(query).to_dataframe().to_dict(orient="records")
    print(f"[AEGIS] Loaded {len(targets)} candidate prime contractor entities.")

    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    headers = {
        "User-Agent": "HoloEarthAnalytics DefenseProcurementOps@holoearthdata.org",
        "Content-Type": "application/json"
    }

    start_date = (datetime.utcnow() - timedelta(days=10*365)).strftime('%Y-%m-%d')
    end_date = datetime.utcnow().strftime('%Y-%m-%d')
    timestamp_iso = datetime.utcnow().isoformat()

    all_awards = []
    chunk_size = 5000

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
        autodetect=True
    )

    for target in targets:
        company_clean = target['company_name'].split()[0].replace(",", "")
        ticker = target['ticker']

        payload = {
            "filters": {
                "time_period": [{"start_date": start_date, "end_date": end_date}],
                "award_type_codes": ["A", "B", "C", "D"], # Procurement contracts
                "recipient_search_text": [company_clean]
            },
            "fields": [
                "Award ID", "Recipient Name", "Award Amount", 
                "Description", "Action Date", "Awarding Agency"
            ],
            "limit": 100,
            "page": 1,
            "sort": "Action Date",
            "order": "desc"
        }

        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=20)
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                for award in results:
                    all_awards.append({
                        "timestamp": timestamp_iso,
                        "domain": "AEGIS",
                        "entity_id": ticker,
                        "recipient_name": award.get("Recipient Name", "UNKNOWN"),
                        "award_amount": float(award.get("Award Amount") or 0.0),
                        "awarding_agency": str(award.get("Awarding Agency", "UNKNOWN")),
                        "action_date": award.get("Action Date", ""),
                        "award_id": str(award.get("Award ID", "UNKNOWN")),
                        "signal_type": "FEDERAL_CONTRACT_AWARD"
                    })

                if len(all_awards) >= chunk_size:
                    client.load_table_from_json(all_awards, table_id, job_config=job_config).result()
                    print(f"[AEGIS] Streamed batch of {len(all_awards)} contract records.")
                    all_awards = []
            elif resp.status_code == 429:
                time.sleep(5)
        except Exception:
            pass

        time.sleep(0.2)

    if all_awards:
        client.load_table_from_json(all_awards, table_id, job_config=job_config).result()
        print(f"[AEGIS] Final batch of {len(all_awards)} awards committed to BigQuery.")

    print("[AEGIS] Ingestion complete.")

if __name__ == "__main__":
    main()
    
