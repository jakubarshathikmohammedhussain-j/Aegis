import os
import json
import requests
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.cloud import bigquery
from google.oauth2 import service_account

# Target Tier-1 Primes for Macro Tracking
TARGET_ENTITIES = [
    "LOCKHEED MARTIN", "RAYTHEON", "GENERAL DYNAMICS", 
    "BOEING", "NORTHROP GRUMMAN", "BAE SYSTEMS", "L3HARRIS"
]

def stream_to_bigquery(client, table_id, rows_to_insert):
    if not rows_to_insert:
        return
    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = client.load_table_from_json(rows_to_insert, table_id, job_config=job_config)
        job.result()
        print(f"[BIGQUERY] Successfully batch-loaded {len(rows_to_insert)} AEGIS records.")
    except Exception as e:
        print(f"[BIGQUERY ERROR] {e}")

def main():
    print("[AEGIS Node] Initializing daily federal procurement sync...")

    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.aegis_procurement"

    # Set time window (last 48 hours to account for federal reporting delays)
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=2)
    
    print(f"[AEGIS] Scanning federal awards from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    payload = {
        "filters": {
            "time_period": [{"start_date": start_date.strftime('%Y-%m-%d'), "end_date": end_date.strftime('%Y-%m-%d')}],
            "award_type_codes": ["A", "B", "C", "D"] # Prime Contracts
        },
        "fields": ["Recipient Name", "Award Amount", "Awarding Agency", "Start Date"],
        "limit": 100
    }
    headers = {"Content-Type": "application/json"}

    bq_payload = []
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        for award in data.get("results", []):
            recipient = str(award.get("Recipient Name", "")).upper()
            
            # Filter for our target enterprise primes
            if any(prime in recipient for prime in TARGET_ENTITIES):
                amount = award.get("Award Amount")
                
                # Sanitize data before BigQuery insertion
                if amount is None or amount <= 0:
                    continue
                    
                bq_payload.append({
                    "entity_id": recipient,
                    "award_amount": float(amount),
                    "awarding_agency": str(award.get("Awarding Agency", "UNKNOWN")),
                    "date_signed": str(award.get("Start Date", start_date.strftime('%Y-%m-%d')))
                })
                
    except Exception as e:
        print(f"[AEGIS ERROR] API Fetch failure: {e}")

    if bq_payload:
        stream_to_bigquery(client, table_id, bq_payload)
    else:
        print("[AEGIS] No new target contracts found today.")

    # Email Dispatch
    sender_email = os.environ.get('GMAIL_USER')
    sender_password = os.environ.get('GMAIL_APP_PASSWORD')
    recipient_email = os.environ.get('RECIPIENT_EMAIL', sender_email)

    if not sender_email or not sender_password or len(bq_payload) == 0:
        return

    # Sort by largest contract value
    sorted_awards = sorted(bq_payload, key=lambda x: x['award_amount'], reverse=True)

    msg = MIMEMultipart()
    msg['From'] = f"AEGIS Intelligence Node <{sender_email}>"
    msg['To'] = recipient_email
    msg['Subject'] = f"AEGIS Procurement Alert: {len(bq_payload)} New Federal Contracts"

    html_content = f"""
    <div style="font-family: Arial, sans-serif; color: #111; max-width: 600px; line-height: 1.5;">
        <h2 style="color: #b45309; margin-bottom: 8px;">AEGIS Terminal: Federal Procurement Sync</h2>
        <p style="color: #586069; font-size: 14px; margin-top: 0;">Executed at {end_date.isoformat()} UTC</p>
        <p>Successfully captured <b>{len(bq_payload)}</b> new Tier-1 prime contracts into BigQuery.</p>
        <hr style="border: 0; border-top: 1px solid #e1dfd5; margin: 16px 0;">
        <table style='width: 100%; border-collapse: collapse; font-size: 13px;'>
            <tr style='background-color: #fef3c7; text-align: left;'>
                <th style='padding: 6px; border: 1px solid #d1d5db;'>Entity</th>
                <th style='padding: 6px; border: 1px solid #d1d5db;'>Agency</th>
                <th style='padding: 6px; border: 1px solid #d1d5db;'>Amount (USD)</th>
                <th style='padding: 6px; border: 1px solid #d1d5db;'>Date Signed</th>
            </tr>
    """
    
    for item in sorted_awards[:10]: # Show top 10 in email
        formatted_amount = f"${item['award_amount']:,.2f}"
        html_content += f"<tr><td style='padding: 6px; border: 1px solid #d1d5db;'><b>{item['entity_id']}</b></td><td style='padding: 6px; border: 1px solid #d1d5db;'>{item['awarding_agency']}</td><td style='padding: 6px; border: 1px solid #d1d5db; color: #047857;'>{formatted_amount}</td><td style='padding: 6px; border: 1px solid #d1d5db;'>{item['date_signed']}</td></tr>"
    
    html_content += "</table></div>"
    msg.attach(MIMEText(html_content, 'html'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
    except Exception as e:
        print(f"[AEGIS ERROR] Email dispatch failed: {e}")

if __name__ == "__main__":
    main()
    
