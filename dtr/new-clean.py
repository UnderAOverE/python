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
from typing import List, Dict, Optional, Set

def find_and_kill_long_running_processes(config_file: str) -> None:
    """
    Reads process configurations from a JSON file, finds and kills long-running processes.
    Sends email reports directly, consolidating reports for the same email addresses.

    Args:
        config_file (str): Path to the JSON configuration file.
    """

    # Stores the killed processes, grouped by email addresses
    email_reports: Dict[str, List[Dict[str, any]]] = {}
    hostname: str = socket.gethostname()

    try:
        with open(config_file, 'r') as f:
            process_configs: List[Dict[str, any]] = json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found.")
        return
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format in '{config_file}'. The actual error is: {e}")
        return

    for config in process_configs:
        job_name: Optional[str] = config.get("job_name")  # Use .get() to handle missing keys gracefully
        max_runtime_seconds: Optional[int] = config.get("maximum_runtime_in_seconds")
        username: Optional[str] = config.get("username")
        email_addresses: List[str] = config.get("email", [])  # Default to an empty list if email is missing.
        EMAIL_CONFIG: Dict[str, str] = config.get("EMAIL_CONFIG", {})  #Added
        if not all([job_name, max_runtime_seconds]):  # Validate we have a valid config item.
            print(f"Invalid config item: Missing job_name or maximum_runtime_in_seconds for this config.")
            continue  # Skip to the next config item.

        for process in psutil.process_iter(['pid', 'name', 'create_time', 'username', 'cmdline', 'exe']):  # Added exe for executable path
            try:
                process_info: Dict[str, any] = process.info
                pid: int = process_info['pid']
                name: str = process_info['name']
                create_time: float = process_info['create_time']
                username_process: str = process_info['username']
                cmdline: List[str] = process_info['cmdline']
                exe: str = process_info['exe']  # Executable path

                if name == 'python':  # Only look for python processes.

                    # Check if the script path is in the command line arguments
                    for arg in cmdline:

                        if job_name in arg:  # If the job_name is found in one of the command line arguments then its a match.
                            if user is None or username_process == username:
                                current_time: float = time.time()
                                runtime_seconds: float = current_time - create_time

                                if runtime_seconds > max_runtime_seconds:
                                    print(f"Killing process: PID={pid}, Name={name}, Script={job_name}, Runtime={runtime_seconds:.2f} seconds")
                                    try:
                                        os.kill(pid, signal.SIGTERM)  # Graceful termination
                                        time.sleep(2)
                                        if psutil.pid_exists(pid):
                                            print(f"Process {pid} did not terminate gracefully. Sending SIGKILL.")
                                            os.kill(pid, signal.SIGKILL)  # Forceful termination

                                        killed_process_info: Dict[str, any] = {
                                            "job_name": job_name,
                                            "maximum_runtime_in_seconds": max_runtime_seconds,
                                            "username": username,
                                            "current_runtime_in_seconds": runtime_seconds,
                                            "log_datetime": datetime.datetime.utcnow().isoformat(),
                                            "servername": hostname,
                                            "path": exe,
                                            "pid": pid,
                                            "cmdline": cmdline
                                        }
                                        # Group by email addresses
                                        for email in email_addresses:
                                            if email not in email_reports:
                                                email_reports[email] = []
                                            email_reports[email].append(killed_process_info)

                                    except OSError as e:
                                        print(f"Error killing process {pid}: {e}")
                                    break  # Only kill the process once if multiple arguments contain the script name

            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
    for email, report_data in email_reports.items():
        send_email_report(report_data, config.get("EMAIL_CONFIG"))


def send_email_report(report_data: List[Dict[str, any]], email_config: Dict[str, str]) -> None:
    """
    Sends an HTML email report with the details of killed processes in a table format.

    Args:
        report_data (List[Dict[str, any]]): A list of dictionaries containing the report data.
        email_config (Dict[str, str]): A dictionary containing email configuration details (sender, recipient(s), SMTP server, etc.).
    """
    sender_email: str = email_config.get("sender_email")
    sender_password: str = email_config.get("sender_password")
    recipient_emails: List[str] = [email_config.get("recipient_email")]  # Sending email to config email not dynamic one
    smtp_server: str = email_config.get("smtp_server")
    smtp_port: int = int(email_config.get("smtp_port", 587))  # Default port is 587 for TLS.

    if not all([sender_email, sender_password, recipient_emails, smtp_server]):
        print("Error: Missing email configuration details.")
        return  # Stop execution if config is bad.

    try:
        message: MIMEMultipart = MIMEMultipart("alternative")
        message["Subject"] = "Long-Running Processes Killed Report"
        message["From"] = sender_email
        message["To"] = ", ".join(recipient_emails)  # Join multiple recipients
        # HTML email body with table format
        html: str = """
        <html>
        <body>
            <p>The following long-running processes were killed:</p>
            <table border="1">
                <thead>
                    <tr>
                        <th>Job Name</th>
                        <th>PID</th>
                        <th>Runtime (seconds)</th>
                        <th>Server</th>
                        <th>Path</th>
                        <th>Command Line</th>
                    </tr>
                </thead>
                <tbody>
        """

        for process_info in report_data:
            html += f"""
                    <tr>
                        <td>{process_info['job_name']}</td>
                        <td>{process_info['pid']}</td>
                        <td>{process_info['current_runtime_in_seconds']:.2f}</td>
                        <td>{process_info['servername']}</td>
                        <td>{process_info['path']}</td>
                        <td>{process_info['cmdline']}</td>
                    </tr>
            """

        html += """
                </tbody>
            </table>
        </body>
        </html>
        """

        part = MIMEText(html, "html")
        message.attach(part)

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Secure the connection
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_emails, message.as_string())

        print(f"Email report sent successfully to {recipient_emails}.")

    except Exception as e:
        print(f"Error sending email: {e}")
def validate_json_file(file_path: str) -> None:
  """
  Validates the JSON syntax of a file and prints any errors.

  Args:
    file_path (str): The path to the JSON file to validate.
  """
  try:
    with open(file_path, 'r') as f:
      json.load(f)
    print(f"JSON file '{file_path}' is valid.")
  except json.JSONDecodeError as e:
    print(f"JSONDecodeError in '{file_path}': {e}")
  except FileNotFoundError:
    print(f"FileNotFoundError: File '{file_path}' not found.")
  except Exception as e:
    print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    CONFIG_FILE: str = "process_config.json"
    validate_json_file(CONFIG_FILE)  # Validate the JSON file before running the process killer
    find_and_kill_long_running_processes(CONFIG_FILE)
