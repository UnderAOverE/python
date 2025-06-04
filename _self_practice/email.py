# core/email_service.py
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)

class EmailConfig:
    def __init__(self, smtp_server: str, smtp_port: int, smtp_user: Optional[str] = None,
                 smtp_password: Optional[str] = None, use_tls: bool = True,
                 sender_email: Optional[str] = None): # sender_email can be set here or per email
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.use_tls = use_tls
        self.sender_email = sender_email

class EmailService:
    def __init__(self, config: EmailConfig):
        self.config = config

    def _generate_html_table(self, title: str, details: Dict[str, str]) -> str:
        """Generates an HTML table from a dictionary."""
        if not details:
            return f"<h2>{title}</h2><p>No details provided.</p>"

        table_rows = ""
        for key, value in details.items():
            # Basic HTML escaping for value to prevent XSS if details come from untrusted sources
            # For more robust escaping, consider a library like `html.escape`
            escaped_value = str(value).replace('&', '&').replace('<', '<').replace('>', '>')
            table_rows += f"<tr><td style='padding: 8px; border: 1px solid #ddd; text-align: left; font-weight: bold;'>{key}</td><td style='padding: 8px; border: 1px solid #ddd; text-align: left;'>{escaped_value}</td></tr>\n"

        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h2 {{ color: #333; }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 15px;
                    box-shadow: 0 2px 3px rgba(0,0,0,0.1);
                }}
                th, td {{
                    padding: 12px;
                    border: 1px solid #ddd;
                    text-align: left;
                }}
                th {{
                    background-color: #f2f2f2;
                    color: #333;
                }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                tr:hover {{ background-color: #f1f1f1; }}
            </style>
        </head>
        <body>
            <h2>{title}</h2>
            <table>
                <thead>
                    <tr>
                        <th style='width: 30%;'>Stage/Detail</th>
                        <th>Result/Value</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
            <p style="font-size: 0.9em; color: #777; margin-top: 20px;">
                This is an automated email.
            </p>
        </body>
        </html>
        """
        return html_content

    def send_email(self,
                   to_emails: Union[str, List[str]],
                   subject: str,
                   details_dict: Dict[str, str],
                   table_title: str = "Details Report",
                   sender_email: Optional[str] = None,
                   cc_emails: Optional[Union[str, List[str]]] = None,
                   bcc_emails: Optional[Union[str, List[str]]] = None) -> bool:
        """
        Sends an email with the details formatted as an HTML table.

        Args:
            to_emails: A single email address string or a list of email strings.
            subject: The subject of the email.
            details_dict: The dictionary to be formatted into a table.
            table_title: The title to appear above the table in the email.
            sender_email: Overrides the default sender email from config if provided.
            cc_emails: Optional CC recipients (string or list).
            bcc_emails: Optional BCC recipients (string or list).

        Returns:
            True if the email was sent successfully, False otherwise.
        """
        final_sender_email = sender_email or self.config.sender_email
        if not final_sender_email:
            logger.error("Sender email not configured or provided.")
            return False

        if isinstance(to_emails, str):
            to_emails_list = [to_emails]
        else:
            to_emails_list = to_emails

        all_recipients = list(to_emails_list) # Start with TO recipients

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = final_sender_email
        msg['To'] = ", ".join(to_emails_list)

        if cc_emails:
            cc_list = [cc_emails] if isinstance(cc_emails, str) else cc_emails
            msg['Cc'] = ", ".join(cc_list)
            all_recipients.extend(cc_list)
        
        if bcc_emails:
            bcc_list = [bcc_emails] if isinstance(bcc_emails, str) else bcc_emails
            # BCC recipients are not added to the 'Bcc' header for privacy
            all_recipients.extend(bcc_list)


        html_body = self._generate_html_table(table_title, details_dict)
        
        # Create a plain text version as a fallback (optional but good practice)
        plain_text_parts = [f"{table_title}\n" + "="*len(table_title)]
        for key, value in details_dict.items():
            plain_text_parts.append(f"- {key}: {value}")
        plain_text_body = "\n".join(plain_text_parts) + "\n\nThis is an automated email."

        msg.attach(MIMEText(plain_text_body, 'plain'))
        msg.attach(MIMEText(html_body, 'html'))

        try:
            logger.info(f"Attempting to send email to {', '.join(to_emails_list)} from {final_sender_email} "
                        f"via {self.config.smtp_server}:{self.config.smtp_port}")
            
            if self.config.use_tls: # Typically for port 587
                server = smtplib.SMTP(self.config.smtp_server, self.config.smtp_port)
                server.ehlo() # Extended Hello
                server.starttls() # Secure the connection
                server.ehlo() # Re-identify ourselves as an ESMTP client
            else: # Typically for port 465 (SMTPS) or 25 (no encryption)
                if self.config.smtp_port == 465: # SMTPS (SSL from the start)
                    server = smtplib.SMTP_SSL(self.config.smtp_server, self.config.smtp_port)
                else: # Port 25 or other non-encrypted/non-SMTPS
                    server = smtplib.SMTP(self.config.smtp_server, self.config.smtp_port)
            
            if self.config.smtp_user and self.config.smtp_password:
                logger.info(f"Logging in as {self.config.smtp_user}")
                server.login(self.config.smtp_user, self.config.smtp_password)
            
            server.sendmail(final_sender_email, all_recipients, msg.as_string())
            server.quit()
            logger.info("Email sent successfully.")
            return True
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP Authentication Error: {e}. Check username/password.")
        except smtplib.SMTPServerDisconnected as e:
            logger.error(f"SMTP Server Disconnected: {e}. Server might have gone down or closed connection.")
        except smtplib.SMTPConnectError as e:
            logger.error(f"SMTP Connection Error: {e}. Check server address and port.")
        except smtplib.SMTPException as e:
            logger.error(f"SMTP General Error: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while sending email: {e}")
        
        return False


# main.py
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

from core.email_service import EmailConfig, EmailService # Adjust import if needed

def run_email_test():
    # --- Configuration ---
    # Replace with your actual SMTP server details
    # For Gmail, you might need an "App Password" if 2FA is enabled.
    # Port 587 (TLS) or 465 (SSL) are common.
    
    # Example for Gmail (requires App Password if 2FA is on)
    email_cfg = EmailConfig(
        smtp_server="smtp.gmail.com",
        smtp_port=587, # Use 465 for SMTP_SSL
        smtp_user="your_email@gmail.com",
        smtp_password="your_app_password_or_regular_password",
        use_tls=True, # True for port 587, False if port is 465 (then SMTP_SSL is used)
        sender_email="your_email@gmail.com" # Can be overridden in send_email
    )

    # Example for a local SMTP server like MailHog (great for testing)
    # docker run -d -p 1025:1025 -p 8025:8025 mailhog/mailhog
    # mailhog_cfg = EmailConfig(
    #     smtp_server="localhost",
    #     smtp_port=1025,
    #     use_tls=False, # MailHog doesn't use TLS by default on 1025
    #     sender_email="testsender@example.com"
    # )
    # email_service = EmailService(mailhog_cfg) # Use this for MailHog

    email_service = EmailService(email_cfg) # Use this for Gmail or other real SMTP

    # --- Details for the email ---
    process_details = {
        "Data Ingestion": "Completed successfully. 1000 records processed.",
        "Data Validation": "Completed with 5 minor warnings. See logs for details.",
        "Data Transformation": "Completed. Output written to 'transformed_data.csv'.",
        "Model Training": "In Progress - Epoch 50/100.",
        "Reporting": "Scheduled for 03:00 AM UTC.",
        "Error Summary": "No critical errors reported."
    }
    
    complex_details = {
        "Setup Phase": "All systems nominal.",
        "Execution - Step 1": "Finished. Output: <a href='http://example.com/results1'>Link to Results</a>",
        "Execution - Step 2 (with HTML)": "Partial success. <i>Some items failed.</i> <b>Check logs!</b>",
        "Finalization": "Pending user approval."
    }


    # --- Sending the email ---
    recipient_emails = ["recipient1@example.com", "recipient2@example.com"]
    # For testing, use an email address you can access.
    # If using MailHog, any recipient address will work, and you can view it at http://localhost:8025

    subject = "Nightly Process Report"
    
    success = email_service.send_email(
        to_emails="your_test_recipient_email@example.com", # Change this
        subject="Automated Process Status Report",
        details_dict=process_details,
        table_title="Nightly Batch Process Summary"
    )

    if success:
        print("Report email sent successfully!")
    else:
        print("Failed to send report email.")

    # Example with CC and BCC
    success_complex = email_service.send_email(
        to_emails="primary_recipient@example.com", # Change this
        cc_emails=["cc_recipient@example.com"],
        bcc_emails="bcc_hidden_recipient@example.com", # Can be a list too
        subject="Complex Details with CC/BCC",
        details_dict=complex_details,
        table_title="Complex Process Stages",
        sender_email="custom_sender@example.com" # Override sender
    )
    if success_complex:
        print("Complex details email sent successfully!")
    else:
        print("Failed to send complex details email.")


if __name__ == "__main__":
    run_email_test()
