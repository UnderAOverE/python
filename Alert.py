from pymongo import MongoClient
from datetime import datetime, timedelta
from collections import defaultdict
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['your_database']
collection = db['your_collection']

# Define the date threshold (7 days ago)
date_threshold = datetime.now() - timedelta(days=7)

# Find all documents with a pending start operation
pending_starts = collection.find({
    "operation_details.start.status": "pending",
    "operation_details.stop.log_date": {"$lt": date_threshold}
})

# Group documents by the stop user
grouped_by_user = defaultdict(list)
for doc in pending_starts:
    stop_user = doc["operation_details"]["stop"]["user"]
    grouped_by_user[stop_user].append(doc)

# Close the MongoDB connection
client.close()

# Function to convert datetime to human-readable format
def format_datetime(dt):
    return dt.strftime("%B %d, %Y %H:%M:%S")

# Function to send email
def send_email(to_email, subject, html_content):
    from_email = "your_email@gmail.com"
    password = "your_password"

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email

    part = MIMEText(html_content, 'html')
    msg.attach(part)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(from_email, password)
        server.sendmail(from_email, to_email, msg.as_string())

# Generate and send HTML emails for each user
for user, docs in grouped_by_user.items():
    email = f"{user}@gmail.com"
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Pending Operations Report</title>
        <style>
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            th, td {{
                border: 1px solid black;
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
            td {{
                word-wrap: break-word;
            }}
        </style>
    </head>
    <body>
        <h2>Pending Operations Report</h2>
        <h3>User: {email}</h3>
        <table>
            <tr>
                <th>Cluster</th>
                <th>Application</th>
                <th>Stop Date</th>
            </tr>
    """
    for doc in docs:
        cluster = doc["cluster"]
        application = doc["application"]
        stop_date = doc["operation_details"]["stop"]["log_date"]
        stop_date_str = format_datetime(stop_date)
        html_content += f"""
            <tr>
                <td>{cluster}</td>
                <td>{application}</td>
                <td>{stop_date_str}</td>
            </tr>
        """
    html_content += """
        </table>
    </body>
    </html>
    """

    # Send email to the user
    send_email(email, "Pending Operations Report", html_content)
