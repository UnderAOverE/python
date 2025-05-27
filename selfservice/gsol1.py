import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
import os # For environment variables, though we'll mostly use direct values here

# --- Configuration (Normally from .env or config files) ---

# --- SMTP Configuration ---
# For a real application, load these from environment variables or a secure config system.
# Example .env content (if you were to use it with python-dotenv):
# SMTP_SERVER="smtp.example.com"
# SMTP_PORT=587
# SMTP_USERNAME="your_email@example.com"
# SMTP_PASSWORD="your_email_password"
# SMTP_SENDER_EMAIL="notifications@example.com"
# DEFAULT_RECIPIENT_EMAIL="alerts_team@example.com"
# SMTP_USE_TLS="true"
# SMTP_USE_SSL="false"

SMTP_SERVER = os.getenv("SMTP_SERVER", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025)) # Default for local Python SMTP debug server
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SMTP_SENDER_EMAIL = os.getenv("SMTP_SENDER_EMAIL", "failover-bot@localhost")
DEFAULT_RECIPIENT_EMAIL = os.getenv("DEFAULT_RECIPIENT_EMAIL", "admin@localhost")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "false").lower() == "true"

# --- General Application Settings ---
SITE_A_ID = "A"
SITE_B_ID = "B"

# --- Checker Configurations ---
MONGO_CONFIG = {
    "public_config": {"filter_summary": "user_data service", "red_percentage": 1.0},
    "filter": {"service": "user_data", "region": "primary"},
    "red_percentage": 1.0,
    "simulated_num_red": 2,
    "simulated_total_backends": 2
}

SPLUNK_CONFIG = {
    "public_config": {"query_summary": "Customer Login TPS", "tps_threshold": 50},
    "query": "index=auth sourcetype=logins status=success | timechart span=1m count as tps",
    "tps_threshold": 50,
    "simulated_tps": 60
}

APPD_CONFIG = {
    "public_config": {"api_endpoint_summary": "/myapp/tps", "tps_threshold": 100},
    "api_endpoint": "/appdynamics/api/applications/myapp/tps",
    "tps_threshold": 100,
    "simulated_tps": 120,
}

CLOUDLET_API_CONFIG = {
    "status_url": "https://my-infra.com/api/v1/cloudlet/status",
    "cloudlet_url": "https://my-infra.com/api/v1/cloudlet/switch",
    "initial_site": SITE_A_ID
}

# --- Basic Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
# Create a main logger instance for non-class specific logging if needed
main_logger = logging.getLogger("FailoverSystemMain")


# --- Audit Service ---
class AuditService:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def log_event(self, event_type: str, message: str, details: Optional[Dict[str, Any]] = None):
        log_entry = f"[AUDIT] EventType: '{event_type}', Message: '{message}'"
        if details:
            log_entry += f", Details: {details}"
        self.logger.info(log_entry)

# --- Notification Service ---
class NotificationService:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        # SMTP settings are global in this single-file version
        self.smtp_server = SMTP_SERVER
        self.smtp_port = SMTP_PORT
        self.smtp_username = SMTP_USERNAME
        self.smtp_password = SMTP_PASSWORD
        self.sender_email = SMTP_SENDER_EMAIL
        self.use_tls = SMTP_USE_TLS
        self.use_ssl = SMTP_USE_SSL

    def _format_details_as_html_table(self, details: Optional[Dict[str, Any]]) -> str:
        if not details:
            return "<p>No additional details provided.</p>"
        table_html = '<table border="1" style="border-collapse: collapse; width: 100%;">'
        table_html += "<thead><tr><th style='text-align: left; padding: 8px;'>Key</th><th style='text-align: left; padding: 8px;'>Value</th></tr></thead>"
        table_html += "<tbody>"
        for key, value in details.items():
            table_html += f"<tr><td style='padding: 8px;'>{key}</td><td style='padding: 8px;'>{value}</td></tr>"
        table_html += "</tbody></table>"
        return table_html

    def send_notification(self, subject: str, body_text: str, details: Optional[Dict[str, Any]] = None, recipient: Optional[str] = None):
        resolved_recipient = recipient or DEFAULT_RECIPIENT_EMAIL
        if not resolved_recipient:
            self.logger.error("Notification recipient is not configured.")
            return

        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = self.sender_email
        message["To"] = resolved_recipient

        text_part = MIMEText(body_text, "plain")
        message.attach(text_part)

        html_body = f"<html><body><p>{body_text.replace(chr(10), '<br>')}</p>"
        html_body += "<h2>Details:</h2>"
        html_body += self._format_details_as_html_table(details)
        html_body += "</body></html>"
        html_part = MIMEText(html_body, "html")
        message.attach(html_part)

        try:
            if self.use_ssl:
                server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
            else:
                server = smtplib.SMTP(self.smtp_server, self.smtp_port)
                if self.use_tls:
                    server.starttls()
            
            if self.smtp_username and self.smtp_password:
                server.login(self.smtp_username, self.smtp_password)
            
            server.sendmail(self.sender_email, resolved_recipient, message.as_string())
            server.quit()
            self.logger.info(f"Notification email sent to '{resolved_recipient}' with subject: '{subject}'")
            return True
        except smtplib.SMTPAuthenticationError as e:
            self.logger.error(f"SMTP Authentication Error sending email: {e}. Check credentials for {self.smtp_username}.")
        except smtplib.SMTPServerDisconnected as e:
            self.logger.error(f"SMTP Server Disconnected: {e}. Check server address and port.")
        except smtplib.SMTPConnectError as e:
            self.logger.error(f"SMTP Connection Error: {e}. Is the server running at {self.smtp_server}:{self.smtp_port}?")
        except ConnectionRefusedError as e:
             self.logger.error(f"SMTP Connection Refused: {e}. Is the server running and accepting connections on {self.smtp_server}:{self.smtp_port}?")
        except Exception as e:
            self.logger.error(f"Failed to send notification email: {e}", exc_info=True)
        return False

# --- Checkers (Chain of Responsibility) ---
class AbstractCheckerHandler(ABC):
    def __init__(self, name: str, config: Dict[str, Any], audit_service: AuditService, successor: Optional['AbstractCheckerHandler'] = None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.name = name
        self.config = config
        self.audit_service = audit_service
        self._successor: Optional['AbstractCheckerHandler'] = successor

    @abstractmethod
    def _perform_specific_check(self) -> bool:
        pass

    def handle_check(self) -> bool:
        public_config_details = self.config.get("public_config", self.config)
        self.audit_service.log_event(
            event_type="ConditionCheckStarted",
            message=f"Attempting check for '{self.name}'.",
            details={"checker_name": self.name, "config_summary": public_config_details}
        )
        check_passed = False
        try:
            check_passed = self._perform_specific_check()
        except Exception as e:
            self.logger.error(f"Exception during check '{self.name}': {e}", exc_info=True)
            self.audit_service.log_event(
                event_type="ConditionCheckError",
                message=f"Error during check for '{self.name}'.",
                details={"checker_name": self.name, "error": str(e)}
            )
            return False

        if check_passed:
            self.audit_service.log_event(
                event_type="ConditionCheckPassed",
                message=f"Check PASSED for '{self.name}'.",
                details={"checker_name": self.name}
            )
            if self._successor:
                self.logger.info(f"CoR: Passing from '{self.name}' to successor: '{self._successor.name}'.")
                return self._successor.handle_check()
            else:
                self.logger.info(f"CoR: End of chain reached at '{self.name}', all prior checks passed.")
                return True
        else:
            self.audit_service.log_event(
                event_type="ConditionCheckFailed",
                message=f"Check FAILED for '{self.name}'. Stopping chain.",
                details={"checker_name": self.name}
            )
            return False

class MongoStatusHandler(AbstractCheckerHandler):
    def _perform_specific_check(self) -> bool:
        self.logger.info(f"Performing Mongo check: {self.config.get('filter', {})}, threshold: {self.config.get('red_percentage')}")
        num_red = self.config.get("simulated_num_red", 0)
        total_backends = self.config.get("simulated_total_backends", 1)
        red_percentage_threshold = self.config.get("red_percentage", 1.0)
        if total_backends == 0:
            self.logger.warning(f"Mongo Check '{self.name}': No backends found.")
            return True # Or False, based on requirements
        actual_red_percentage = num_red / total_backends
        result = actual_red_percentage >= red_percentage_threshold
        self.logger.info(f"Mongo Check '{self.name}': {num_red}/{total_backends} RED ({actual_red_percentage*100:.2f}%). Met threshold: {result}")
        return result

class SplunkTPSHandler(AbstractCheckerHandler):
    def _perform_specific_check(self) -> bool:
        self.logger.info(f"Performing Splunk TPS check: query='{self.config.get('query')}', threshold={self.config.get('tps_threshold')}")
        current_tps = self.config.get("simulated_tps", 0)
        threshold = self.config.get("tps_threshold", 0)
        result = current_tps > threshold
        self.logger.info(f"Splunk Check '{self.name}': Current TPS={current_tps}. Met threshold: {result}")
        return result

class AppDynamicsTPSHandler(AbstractCheckerHandler):
    def _perform_specific_check(self) -> bool:
        self.logger.info(f"Performing AppDynamics TPS check: endpoint='{self.config.get('api_endpoint')}', threshold={self.config.get('tps_threshold')}")
        current_tps = self.config.get("simulated_tps", 0)
        threshold = self.config.get("tps_threshold", 0)
        result = current_tps > threshold
        self.logger.info(f"AppDynamics Check '{self.name}': Current TPS={current_tps}. Met threshold: {result}")
        return result

# --- Actions ---
class CloudletAPIClient:
    def __init__(self, config: Dict[str, Any], audit_service: AuditService):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.audit_service = audit_service
        self.simulated_current_site = str(config.get("initial_site", SITE_A_ID))

    def get_current_site(self) -> str:
        status_url = self.config.get('status_url')
        self.audit_service.log_event("APICallAttempt", f"Calling API GET {status_url} to check current site.")
        # Actual API call logic would go here. For now, using simulated value.
        self.logger.info(f"Cloudlet API Status (Simulated): Site {self.simulated_current_site} is active.")
        self.audit_service.log_event("APICallSuccess", f"API GET {status_url} reported site (Simulated): {self.simulated_current_site}.")
        return self.simulated_current_site

    def switch_site(self, current_site_expected:str, target_site: str) -> bool:
        cloudlet_url = self.config.get('cloudlet_url')
        payload = {'current_site': current_site_expected, 'target_site': target_site}
        self.audit_service.log_event(
            "APICallAttempt",
            f"Calling API POST {cloudlet_url} to switch from {current_site_expected} to {target_site}.",
            details=payload
        )
        # Actual API call logic. Simulation:
        if str(self.simulated_current_site) == str(current_site_expected):
            self.simulated_current_site = str(target_site)
            self.logger.info(f"Cloudlet API (Simulated) successfully switched from {current_site_expected} to Site {target_site}.")
            self.audit_service.log_event("APICallSuccess", f"API POST {cloudlet_url} switched site (Simulated) to {target_site}.")
            return True
        else:
            reason = f"Site mismatch. Expected current site {current_site_expected}, but (simulated) API reports {self.simulated_current_site}."
            self.logger.warning(f"Cloudlet API (Simulated) switch failed: {reason}")
            self.audit_service.log_event("APICallFailure", f"API POST {cloudlet_url} failed to switch site (Simulated).", {"reason": reason})
            return False

# --- Core Orchestrator ---
class FailoverManager:
    def __init__(self,
                 first_condition_handler: AbstractCheckerHandler,
                 cloudlet_api_client: CloudletAPIClient,
                 audit_service: AuditService,
                 notification_service: NotificationService,
                 site_a_id: str,
                 site_b_id: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.first_condition_handler = first_condition_handler
        self.cloudlet_api_client = cloudlet_api_client
        self.audit_service = audit_service
        self.notification_service = notification_service
        self.site_a_identifier = str(site_a_id)
        self.site_b_identifier = str(site_b_id)
        self.failover_performed_by_this_process: bool = False

    def _all_pre_failover_conditions_met(self) -> bool:
        self.audit_service.log_event("ProcessStep", "Starting pre-failover condition checks via CoR.")
        return self.first_condition_handler.handle_check()

    def attempt_failover(self):
        process_details = {"target_failover_from": self.site_a_identifier, "target_failover_to": self.site_b_identifier}
        self.audit_service.log_event("FailoverProcessStarted", "Attempting automated failover.", process_details)
        self.logger.info("========== ATTEMPTING FAILOVER ==========")

        current_site = str(self.cloudlet_api_client.get_current_site())
        if current_site != self.site_a_identifier:
            msg = f"Failover not initiated: System is currently on Site '{current_site}' (expected '{self.site_a_identifier}')."
            self.logger.info(msg)
            details = {"current_site": current_site, "expected_site": self.site_a_identifier, "reason": "Not on primary site"}
            self.audit_service.log_event("FailoverProcessAborted", msg, details)
            return

        if not self._all_pre_failover_conditions_met():
            msg = "Failover aborted due to one or more failed condition checks."
            self.logger.warning(msg)
            details = {"reason": "Condition check(s) failed"}
            self.audit_service.log_event("FailoverProcessAborted", msg, details)
            self.notification_service.send_notification(
                subject=f"Automated Failover Aborted ({self.site_a_identifier} -> {self.site_b_identifier}): Condition Check Failed",
                body_text=msg + " Review audit logs for specific failed checks.",
                details=details
            )
            return

        current_site_before_action = str(self.cloudlet_api_client.get_current_site())
        if current_site_before_action != self.site_a_identifier:
            msg = f"Failover aborted: Site status changed to '{current_site_before_action}' during checks (expected '{self.site_a_identifier}'). Possible manual intervention or race condition."
            self.logger.warning(msg)
            details = {"current_site_pre_action": current_site_before_action, "expected_site": self.site_a_identifier, "reason": "Site status changed mid-process"}
            self.audit_service.log_event("FailoverProcessAborted", msg, details)
            self.notification_service.send_notification(
                subject=f"Automated Failover Aborted ({self.site_a_identifier} -> {self.site_b_identifier}): Site Status Changed",
                body_text=msg,
                details=details
            )
            return

        self.logger.info(f"All checks passed and site is still '{self.site_a_identifier}'. Proceeding with failover action.")
        action_details = {"from_site": self.site_a_identifier, "to_site": self.site_b_identifier}
        if self.cloudlet_api_client.switch_site(self.site_a_identifier, self.site_b_identifier):
            msg = f"Successfully failed over from Site '{self.site_a_identifier}' to Site '{self.site_b_identifier}'."
            self.logger.info(msg)
            self.failover_performed_by_this_process = True
            self.audit_service.log_event("FailoverProcessSuccess", msg, action_details)
            self.notification_service.send_notification(
                subject=f"Automated Failover Successful: {self.site_a_identifier} -> {self.site_b_identifier}",
                body_text=msg,
                details=action_details
            )
        else:
            msg = f"Failover API call to switch to Site '{self.site_b_identifier}' FAILED."
            self.logger.error(msg)
            details = {**action_details, "reason": "Cloudlet API switch command failed"}
            self.audit_service.log_event("FailoverProcessFailure", msg, details)
            self.notification_service.send_notification(
                subject=f"Automated Failover FAILED ({self.site_a_identifier} -> {self.site_b_identifier}): API Error",
                body_text=msg,
                details=details
            )
        self.logger.info("==========================================")

    def attempt_rollback(self):
        process_details = {"target_rollback_from": self.site_b_identifier, "target_rollback_to": self.site_a_identifier}
        self.audit_service.log_event("RollbackProcessStarted", "Attempting automated rollback.", process_details)
        self.logger.info("========== ATTEMPTING ROLLBACK ==========")

        if not self.failover_performed_by_this_process:
            msg = "Rollback not allowed: Previous failover was not performed by this automated process."
            self.logger.warning(msg)
            details = {"reason": "Not initiated by this process", "current_flag_state": self.failover_performed_by_this_process}
            self.audit_service.log_event("RollbackProcessAborted", msg, details)
            return

        current_site = str(self.cloudlet_api_client.get_current_site())
        if current_site != self.site_b_identifier:
            msg = f"Rollback not initiated: System is currently on Site '{current_site}' (expected '{self.site_b_identifier}')."
            self.logger.info(msg)
            details = {"current_site": current_site, "expected_site": self.site_b_identifier, "reason": "Not on secondary site"}
            self.audit_service.log_event("RollbackProcessAborted", msg, details)
            return

        self.logger.info(f"Proceeding with rollback action from Site '{self.site_b_identifier}' to Site '{self.site_a_identifier}'.")
        action_details = {"from_site": self.site_b_identifier, "to_site": self.site_a_identifier}
        if self.cloudlet_api_client.switch_site(self.site_b_identifier, self.site_a_identifier):
            msg = f"Successfully rolled back from Site '{self.site_b_identifier}' to Site '{self.site_a_identifier}'."
            self.logger.info(msg)
            self.failover_performed_by_this_process = False
            self.audit_service.log_event("RollbackProcessSuccess", msg, action_details)
            self.notification_service.send_notification(
                subject=f"Automated Rollback Successful: {self.site_b_identifier} -> {self.site_a_identifier}",
                body_text=msg,
                details=action_details
            )
        else:
            msg = f"Rollback API call to switch to Site '{self.site_a_identifier}' FAILED."
            self.logger.error(msg)
            details = {**action_details, "reason": "Cloudlet API switch command failed"}
            self.audit_service.log_event("RollbackProcessFailure", msg, details)
            self.notification_service.send_notification(
                subject=f"Automated Rollback FAILED ({self.site_b_identifier} -> {self.site_a_identifier}): API Error",
                body_text=msg,
                details=details
            )
        self.logger.info("========================================")


# --- Main Execution Block ---
if __name__ == "__main__":
    main_logger.info("Initializing Failover System Components...")

    audit_service = AuditService()
    notification_service = NotificationService()

    appd_handler = AppDynamicsTPSHandler(
        name="AppDynamics TPS Check",
        config=APPD_CONFIG,
        audit_service=audit_service
    )
    splunk_handler = SplunkTPSHandler(
        name="Splunk Login TPS Check",
        config=SPLUNK_CONFIG,
        audit_service=audit_service,
        successor=appd_handler
    )
    mongo_handler = MongoStatusHandler(
        name="Mongo Backend Status Check",
        config=MONGO_CONFIG,
        audit_service=audit_service,
        successor=splunk_handler
    )
    first_checker_in_chain = mongo_handler

    cloudlet_client = CloudletAPIClient(
        config=CLOUDLET_API_CONFIG,
        audit_service=audit_service
    )

    failover_manager = FailoverManager(
        first_condition_handler=first_checker_in_chain,
        cloudlet_api_client=cloudlet_client,
        audit_service=audit_service,
        notification_service=notification_service,
        site_a_id=SITE_A_ID,
        site_b_id=SITE_B_ID
    )

    main_logger.info("\n--- SCENARIO 1: Attempting Failover (All Conditions Met) ---")
    failover_manager.attempt_failover()

    main_logger.info("\n--- SCENARIO 2: Attempting Rollback (Should Work) ---")
    failover_manager.attempt_rollback()

    main_logger.info("\n--- SCENARIO 3: Attempting Rollback Again (Should Not Work - flag is false) ---")
    failover_manager.attempt_rollback()

    main_logger.info("\n--- SCENARIO 4: Attempting Failover (Splunk TPS Low - condition fails) ---")
    splunk_config_fail = SPLUNK_CONFIG.copy()
    splunk_config_fail["simulated_tps"] = 30
    splunk_config_fail["public_config"]["tps_threshold"] = 30

    appd_handler_s4 = AppDynamicsTPSHandler(name="AppDynamics TPS Check", config=APPD_CONFIG, audit_service=audit_service)
    splunk_handler_s4_fail = SplunkTPSHandler(name="Splunk Login TPS Check (Low TPS)", config=splunk_config_fail, audit_service=audit_service, successor=appd_handler_s4)
    mongo_handler_s4 = MongoStatusHandler(name="Mongo Backend Status Check", config=MONGO_CONFIG, audit_service=audit_service, successor=splunk_handler_s4_fail)
    
    cloudlet_client.simulated_current_site = SITE_A_ID # Ensure starting from Site A for this test
    failover_manager_s4 = FailoverManager(
        first_condition_handler=mongo_handler_s4,
        cloudlet_api_client=cloudlet_client,
        audit_service=audit_service,
        notification_service=notification_service,
        site_a_id=SITE_A_ID,
        site_b_id=SITE_B_ID
    )
    failover_manager_s4.attempt_failover()

    main_logger.info("\n--- SCENARIO 5: Attempting Failover (Site already on B) ---")
    cloudlet_client.simulated_current_site = SITE_B_ID
    failover_manager.attempt_failover() # Use original manager
    
    cloudlet_client.simulated_current_site = SITE_A_ID # Reset for potential further tests
    main_logger.info("All scenarios completed.")
