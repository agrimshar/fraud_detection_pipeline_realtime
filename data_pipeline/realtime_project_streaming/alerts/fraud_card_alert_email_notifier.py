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

def create_fraud_alert_email_body(alert_data):
    return f"""
        <html>
        <body>

        <h2>Fraud Alert - Watchlist Match Detected</h2>

        <p>Dear {alert_data['customer_name']}</p>

        <p>We detected a transaction on your card that matches a fraud watchlist entry:</p>

        <div>

        <h3>Transaction Details</h3>
        <ul>

        <li><strong>Transaction ID:</strong> {alert_data['transaction_id']}</li>
        <li><strong>Transaction Amount:</strong> {alert_data['amount']} {alert_data['currency']}</li>
        <li><strong>Merchant Name:</strong> {alert_data['merchant_name']}</li>
        <li><strong>Merchant Category:</strong> {alert_data['merchant_category']}</li>
        <li><strong>Transaction Type:</strong> {alert_data['transaction_type']}</li>
        <li><strong>Payment Channel:</strong> {alert_data['payment_channel']}</li>
        <li><strong>Transaction Timestamp:</strong> {alert_data['transaction_timestamp']}</li>
        <li><strong>Location:</strong> {alert_data['transaction_city']}, {alert_data['transaction_country']}</li>
        <li><strong>International Transaction:</strong> {alert_data['is_international']}</li>
        <li><strong>Transaction Status:</strong> {alert_data['transaction_status']}</li>

        </ul>

        <h3>Fraud Watchlist Details</h3>
        <ul>

        <li><strong>Watchlist ID:</strong> {alert_data['watchlist_id']}</li>
        <li><strong>Watch Type:</strong> {alert_data['watch_type']}</li>
        <li><strong>Risk Level:</strong> {alert_data['risk_level']}</li>
        <li><strong>Action:</strong> {alert_data['action']}</li>
        <li><strong>Reason Code:</strong> {alert_data['reason_code']}</li>
        <li><strong>Reason Description:</strong> {alert_data['reason_description']}</li>
        <li><strong>Effective From:</strong> {alert_data['watchlist_effective_from']}</li>
        <li><strong>Reported By:</strong> {alert_data['reported_by']}</li>
        <li><strong>Reported Source:</strong> {alert_data['reported_source']}</li>

        </ul>

        </div>

        <p><strong>Action required:</strong></p>
        <ul>

        <li>If you authorized this transaction, please contact our fraud department to verify.</li>
        <li>If you did NOT authorize this transaction, contact our fraud department immediately.</li>

        </ul>

        <p>For your security, we're monitoring your account closely.</p>

        <hr>

        <p>
        Alert ID: {alert_data['alert_id']}<br>
        Alert Type: {alert_data['alert_type']}<br>
        This is an automated message from the realtime_project Transaction Monitoring System.
        </p>

        </body>
        </html>
    """

@dp.foreach_batch_sink(name="fraud_email_notifier_sink")
def send_fraud_alert_emails(df, batch_id):
    EMAIL_FROM = "#"

    rows = df.collect()
    print(f"Batch {batch_id}: processing {len(rows)} fraud alert(s)...")

    success_count = 0
    failure_count = 0

    for row in rows:
        try:
            alert_data = {
                'alert_id': row.alert_id,
                'alert_type': row.alert_type,
                'customer_name': row.customer_name,
                'transaction_id': row.transaction_id,
                'amount': float(row.amount),
                'currency': row.currency,
                'merchant_name': row.merchant_name,
                'merchant_category': row.merchant_category,
                'transaction_type': row.transaction_type,
                'payment_channel': row.payment_channel,
                'transaction_timestamp': str(row.transaction_timestamp),
                'transaction_city': row.transaction_city,
                'transaction_country': row.transaction_country,
                'is_international': row.is_international,
                'transaction_status': row.transaction_status,
                'watchlist_id': row.watchlist_id,
                'watch_type': row.watch_type,
                'risk_level': row.risk_level,
                'action': row.action,
                'reason_code': row.reason_code,
                'reason_description': row.reason_description,
                'watchlist_effective_from': str(row.watchlist_effective_from),
                'reported_by': row.reported_by,
                'reported_source': row.reported_source
            }

            subject = f"Fraud Alert - {alert_data['alert_id']}"
            body = create_fraud_alert_email_body(alert_data)

            send_email(row.customer_email, subject, body, EMAIL_FROM, APP_PASSWORD)

            success_count += 1
            print(f"Email sent to {row.customer_email} for fraud alert {row.alert_id}")

        except Exception as e:
            failure_count += 1
            print(f"Error processing fraud alert {row.alert_id}: {e}")

        print(f"Batch {batch_id} complete: {success_count} succeeded, {failure_count} failed")

@dp.append_flow(target="fraud_email_notifier_sink")
def fraud_alert_stream():
    """Streaming flow that reads fraud card alerts"""
    return spark.readStream.table("realtime_project.gold.fraud_card_alert")
