import psutil
import time
import os
import signal
import json
import datetime
import socket
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Optional

def find_and_kill_long_running_processes(config_file: str) -> List[Dict[str, any]]:
    """
    Reads process configurations from a JSON file, finds and kills long-running processes,
    and generates a JSON report.

    Args:
        config_file (str): Path to the JSON configuration file.

    Returns:
        List[Dict[str, any]]: A list of dictionaries, where each dictionary represents
        a killed process and contains information like job_name, runtime, servername, etc.
    """

    killed_processes_report: List[Dict[str, any]] = []
    hostname: str = socket.gethostname()

    try:
        with open(config_file, 'r') as f:
            process_configs: List[Dict[str, any]] = json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found.")
        return []  # Return an empty list if the config file is not found
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in '{config_file}'.")
        return []

    for config in process_configs:
        job_name: Optional[str] = config.get("job_name")  # Use .get() to handle missing keys gracefully
        max_runtime_seconds: Optional[int] = config.get("maximum_runtime_in_seconds")
        username: Optional[str] = config.get("username")
        email_addresses: List[str] = config.get("email", [])  # Default to an empty list if email is missing.

        if not all([job_name, max_runtime_seconds]): # Validate we have a valid config item.
            print(f"Invalid config item: Missing job_name or maximum_runtime_in_seconds for this config.")
            continue # Skip to the next config item.

        for process in psutil.process_iter(['pid', 'name', 'create_time', 'username', 'exe']): #Added exe for executable path
            try:
                process_info: Dict[str, any] = process.info
                pid: int = process_info['pid']
                name: str = process_info['name']
                create_time: float = process_info['create_time']
                username_process: str = process_info['username']
                exe: str = process_info['exe'] #Executable path

                if name == job_name and (username is None or username_process == username): # Compare full process name instead of prefix
                    current_time: float = time.time()
                    runtime_seconds: float = current_time - create_time

                    if runtime_seconds > max_runtime_seconds:
                        print(f"Killing process: PID={pid}, Name={name}, Runtime={runtime_seconds:.2f} seconds")
                        try:
                            os.kill(pid, signal.SIGTERM) #Graceful termination

                            time.sleep(2)
                            if psutil.pid_exists(pid):  # Check if the process is still running
                                print(f"Process {pid} did not terminate gracefully. Sending SIGKILL.")
                                os.kill(pid, signal.SIGKILL)  #Forceful termination

                            killed_process_info: Dict[str, any] = {
                                "job_name": job_name,
                                "maximum_runtime_in_seconds": max_runtime_seconds,
                                "username": username,
                                "current_runtime_in_seconds": runtime_seconds,
                                "log_datetime": datetime.datetime.utcnow().isoformat(),
                                "servername": hostname,
                                "path": exe
                            }
                            killed_processes_report.append(killed_process_info)

                        except OSError as e:
                            print(f"Error killing process {pid}: {e}")
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass

    return killed_processes_report


def send_email_report(report_data: List[Dict[str, any]], email_config: Dict[str, str]) -> None:
    """
    Sends an email report with the details of killed processes.

    Args:
        report_data (List[Dict[str, any]]): A list of dictionaries containing the report data.
        email_config (Dict[str, str]): A dictionary containing email configuration details (sender, recipient(s), SMTP server, etc.).

    Returns:
        None
    """
    sender_email: str = email_config.get("sender_email")
    sender_password: str = email_config.get("sender_password")
    recipient_emails: List[str] = email_config.get("recipient_emails")
    smtp_server: str = email_config.get("smtp_server")
    smtp_port: int = email_config.get("smtp_port", 587) # Default port is 587 for TLS.

    if not all([sender_email, sender_password, recipient_emails, smtp_server]):
        print("Error: Missing email configuration details.")
        return #Stop execution if config is bad.

    try:
        message: MIMEMultipart = MIMEMultipart("alternative")
        message["Subject"] = "Long-Running Processes Killed Report"
        message["From"] = sender_email
        message["To"] = ", ".join(recipient_emails) # Join multiple recipients

        text: str = "The following long-running processes were killed:\n\n" # Plain text body
        html: str = """<html><body><p>The following long-running processes were killed:</p><ul>""" # HTML body

        for process_info in report_data:
            text += f"Job Name: {process_info['job_name']}, Runtime: {process_info['current_runtime_in_seconds']:.2f} seconds, Server: {process_info['servername']}, Path: {process_info['path']}\n"
            html += f"""<li>Job Name: {process_info['job_name']}, Runtime: {process_info['current_runtime_in_seconds']:.2f} seconds, Server: {process_info['servername']}, Path: {process_info['path']}</li>"""

        text += "\nEnd of Report"
        html += """</ul></body></html>"""

        part1: MIMEText = MIMEText(text, "plain") # Create the plain-text and HTML parts
        part2: MIMEText = MIMEText(html, "html")

        message.attach(part1) # Attach them to the message
        message.attach(part2)

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Secure the connection
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_emails, message.as_string())

        print("Email report sent successfully.")

    except Exception as e:
        print(f"Error sending email: {e}")


if __name__ == "__main__":
    CONFIG_FILE: str = "process_config.json"
    OUTPUT_FILE: str = "killed_processes_report.json"
    EMAIL_CONFIG: Dict[str, str] = { #Replace with actual values.  Store securely (e.g. environment variables)
        "sender_email": "your_email@gmail.com",
        "sender_password": "your_password",
        "recipient_emails": ["recipient1@example.com", "recipient2@example.com"],
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587  # Or 465 for SSL
    }

    killed_processes: List[Dict[str, any]] = find_and_kill_long_running_processes(CONFIG_FILE)

    # Save the report to a JSON file
    try:
        with open(OUTPUT_FILE, 'w') as outfile:
            json.dump(killed_processes, outfile, indent=4)
        print(f"Report saved to '{OUTPUT_FILE}'.")
    except IOError as e:
        print(f"Error saving report to file: {e}")

    # Send the email report
    if killed_processes:
        send_email_report(killed_processes, EMAIL_CONFIG)
    else:
        print("No processes were killed, so no email was sent.")
