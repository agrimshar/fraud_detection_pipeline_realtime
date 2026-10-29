from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

APP_PASSWORD = dbutils.secrets.get("realtime_project_scope", "gmail_api_key")

def send_email(to_email, subject, body, from_email, app_password):
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "html"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(from_email, app_password)
        server.send_message(msg)

def create_alert_email_body(alert_data):
    return f"""
        <html>
        <body>

        <h2>High Value Transaction Alert</h2>

        <p>Dear {alert_data['customer_name']}</p>

        <p>We detected a transaction that exceeded your configured limit:</p>

        <div>
        
        <h3>Transaction Details</h3>
        <ul>
        
        <li><strong>Transaction ID:</strong> {alert_data['transaction_id']}</li>
        <li><strong>Transaction Amount:</strong> {alert_data['transaction_amount']} {alert_data['currency']}</li>
        <li><strong>Transaction Limit:</strong> {alert_data['transaction_limit']} {alert_data['currency']}</li>
        <li><strong>Merchant Name:</strong> {alert_data['merchant_name']}</li>
        <li><strong>Merchant Category:</strong> {alert_data['merchant_category']}</li>
        <li><strong>Transaction Timestamp:</strong> {alert_data['transaction_timestamp']}</li>
        <li><strong>Location:</strong> {alert_data['city']}, {alert_data['country']}</li>
        
        </ul>

        </div>

        <p><strong>Action required:</strong></p>
        <ul>
        
        <li>If you authroized this transaction, no action is needed.</li>
        <li>If you did NOT authorize this, contact our fraud department immediately.</li>
        
        </ul>

        <p>For your security, we're monitoring your account closely.</p>

        <hr>

        <p>
        Alert ID: {alert_data['alert_id']}<br>
        This is an automated message from the realtime_project Transaction Monitoring System.
        </p>

        </body>
        </html>
    """

@dp.foreach_batch_sink(name = "email_notifier_sink")
def send_high_value_alert_emails(df, batch_id):
    EMAIL_FROM = "#"
    
    rows = df.collect()
    print(f"Batch {batch_id}: processing {len(rows)} alert(s)...")

    success_count = 0
    failure_count = 0

    for row in rows:
        try:
            alert_data = {
                'alert_id': row.alert_id,
                'customer_name': row.customer_name,
                'transaction_id': row.transaction_id,
                'transaction_amount': float(row.transaction_amount),
                'currency': row.currency,
                'transaction_limit': float(row.transaction_limit),
                'merchant_name': row.merchant_name,
                'merchant_category': row.merchant_category,
                'transaction_timestamp': str(row.transaction_timestamp),
                'city': row.city,
                'country': row.country
            }

            subject = f"High Value Transaction Alert - {alert_data['alert_id']}"
            body = create_alert_email_body(alert_data)

            send_email(row.customer_email, subject, body, EMAIL_FROM, APP_PASSWORD)

            success_count += 1
            print(f"Email sent to {row.customer_email} for transaction {row.transaction_id}")
        
        except Exception as e:
            failure_count += 1
            print(f"Error processing alert {row.alert_id}: {e}")
        
        print(f"Batch {batch_id} complete: {success_count} succeeded, {failure_count} failed")

@dp.append_flow(target = "email_notifier_sink")
def high_value_alert_stream():
    """Streaming flow that reads high-value transaction alerts"""
    return spark.readStream.table("realtime_project.gold.high_value_transactions_alert")