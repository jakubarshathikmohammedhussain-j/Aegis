import os
import json
import time
import requests
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

def main():
    print("[AEGIS TIER 1] Initializing 10-Year Federal Contract Backfill...")

    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.aegis_procurement"

    # Fetch Tier 1 candidates (Industrials, Tech, Healthcare)
    query = f"""
        SELECT DISTINCT ticker, company_name 
        FROM `{creds_dict['project_id']}.telemetry_bronze.tier1_master_index`
        WHERE gics_sector IN ('Industrials', 'Information Technology', 'Health Care')
    """
    targets = client.query(query).to_dataframe().to_dict(orient="records")
    print(f"[AEGIS] Loaded {len(targets)} candidate prime contractors.")

    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    headers = {
        "User-Agent": "HoloEarthAnalytics Admin@holoearthdata.org",
        "Content-Type": "application/json"
    }

    start_date = (datetime.utcnow() - timedelta(days=10*365)).strftime('%Y-%m-%d')
    end_date = datetime.utcnow().strftime('%Y-%m-%d')
    timestamp_iso = datetime.utcnow().isoformat()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
        autodetect=True
    )

    all_awards = []

    for target in targets:
        # Clean name for API search (e.g., "Lockheed Martin Corporation" -> "Lockheed Martin")
        company_clean = " ".join(target['company_name'].split()[:2]).replace(",", "")
        ticker = target['ticker']
        
        print(f"[AEGIS] Querying contracts for {ticker} ({company_clean})...")
        
        page = 1
        has_more = True
        
        # Cap at 20 pages (2,000 recent contracts) per entity to prevent API timeouts
        while has_more and page <= 20: 
            payload = {
                "filters": {
                    "time_period": [{"start_date": start_date, "end_date": end_date}],
                    "award_type_codes": ["A", "B", "C", "D"],
                    "recipient_search_text": [company_clean]
                },
                "fields": [
                    "Award ID", "Recipient Name", "Award Amount", 
                    "Description", "Start Date", "Awarding Agency" # Fixed: USASpending uses "Start Date"
                ],
                "limit": 100,
                "page": page,
                "sort": "Start Date",
                "order": "desc"
            }

            try:
                resp = requests.post(url, headers=headers, json=payload, timeout=25)
                if resp.status_code == 200:
                    data = resp.json()
                    results = data.get("results", [])
                    
                    if not results:
                        break
                        
                    for award in results:
                        all_awards.append({
                            "timestamp": timestamp_iso,
                            "domain": "AEGIS",
                            "entity_id": ticker,
                            "recipient_name": str(award.get("Recipient Name", "UNKNOWN")),
                            "award_amount": float(award.get("Award Amount") or 0.0),
                            "awarding_agency": str(award.get("Awarding Agency", "UNKNOWN")),
                            "date_signed": str(award.get("Start Date", "")), # Fixed: Matches BigQuery Schema
                            "award_id": str(award.get("Award ID", "UNKNOWN")),
                            "signal_type": "FEDERAL_CONTRACT_AWARD"
                        })

                    if len(all_awards) >= 3000:
                        client.load_table_from_json(all_awards, table_id, job_config=job_config).result()
                        print(f"[AEGIS] Committed {len(all_awards)} rows to BigQuery.")
                        all_awards = []

                    # API Pagination check
                    if data.get("page_metadata", {}).get("hasNext", False):
                        page += 1
                        time.sleep(0.5)
                    else:
                        has_more = False
                elif resp.status_code == 429:
                    print(f"[AEGIS RATE LIMIT] Sleeping 5s for {ticker}...")
                    time.sleep(5)
                else:
                    print(f"[AEGIS ERROR] HTTP {resp.status_code} for {ticker}")
                    break
            except Exception as e:
                print(f"[AEGIS ERROR] Request/BQ load failed for {ticker}: {e}")
                break
                
    if all_awards:
        client.load_table_from_json(all_awards, table_id, job_config=job_config).result()
        print(f"[AEGIS] Committed final batch of {len(all_awards)} rows.")

    print("[AEGIS] Ingestion complete.")

if __name__ == "__main__":
    main()
    
