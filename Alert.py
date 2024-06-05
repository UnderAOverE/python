# Create HTML content
html_content = """
<!DOCTYPE html>
<html>
<head>
    <title>Pending Operations Report</title>
    <style>
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            border: 1px solid black;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
        }
    </style>
</head>
<body>
    <h2>Pending Operations Report</h2>
"""

for user, docs in grouped_by_user.items():
    email = f"{user}@gmail.com"
    html_content += f"<h3>User: {email}</h3>"
    html_content += """
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
        html_content += f"""
        <tr>
            <td>{cluster}</td>
            <td>{application}</td>
            <td>{stop_date}</td>
        </tr>
        """
    html_content += "</table>"

html_content += """
</body>
</html>
"""

# Save the HTML content to a file
with open('pending_operations_report.html', 'w') as file:
    file.write(html_content)


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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

# Send emails to each user
for user in grouped_by_user.keys():
    email = f"{user}@gmail.com"
    send_email(email, "Pending Operations Report", html_content)
