class APIStatusHandler(CORHandler):
    async def _perform_check(self) -> bool:
        overall_status = False
        api_status_service = await APIService.get_service()
        current_config = await api_status_service.get_failover_config(name=self.name, environment=self.environment)
        getSafeModes_config = current_config.safeMode_config.safeMode_endpoints.getSafeModes

        http_client_config = HttpClientConfig(
            base_url=current_config.safeMode_url,
            timeout=getSafeModes_config.get("timeout"),
            default_headers=getSafeModes_config.get("headers"),
            request_params=getSafeModes_config.get("params"),
            ca_cert_path=getSafeModes_config.get("ca_certificate_path"),
        )

        async with AsyncHttpClient(http_client_config) as client:
            try:
                response = await client.get(getSafeModes_config.get("slug"))
                logger.info(f"Status response: {response.status_code}")
                overall_status = response.status_code == 200
            except HttpClientError as http_client_error:
                logger.error(f"HTTP client error occurred while checking API status: {http_client_error}")

        return overall_status


class FailoverManager:
    def __init__(self, first_condition_handler: CORHandler, notification_service: EmailService) -> None:
        self.first_condition_handler = first_condition_handler
        self.notification_service = notification_service

    async def attempt_failover(self):
        logger.info("------ ATTEMPTING FAILOVER ------")
        status, status_details = await self.first_condition_handler.handle_check()

        if not status:
            logger.error("Failover aborted due to one or more failed condition checks.")
            if self.notification_service.send_email:
                logger.info("Sending failover failure email.")
                await self.notification_service.send_email(
                    subject=f"{self.first_condition_handler.name}: Report: Failover Operation failed",
                    body=status_details,
                    footer_title="Failover Operation Details"
                )
        else:
            logger.info("All pre-failover conditions passed.")
            if self.notification_service.send_email:
                logger.info("Sending failover success email.")
                await self.notification_service.send_email(
                    subject=f"{self.first_condition_handler.name}: Report: Failover Operation Completed",
                    body=status_details,
                    footer_title="Failover Operation Details"
                )


if __name__ == "__main__":
    enable_trace: bool = os.environ.get("LOG_ENABLE_TRACE", False)

    email_service = EmailService()
    api_handler = APIStatusHandler(handler_name="APIStatusHandler", name="SafeMode (IBSRecycle) Failover")

    first_checker = BackendHandler(handler_name="BackendHandler", name="SafeMode (IBSRecycle) Failover", successor=api_handler)

    failover_manager = FailoverManager(first_condition_handler=first_checker, notification_service=email_service)
    asyncio.run(failover_manager.attempt_failover())
    
class CORHandler(ABC):
    def __init__(self, handler_name: str, name: str, successor: Optional["CORHandler"] = None) -> None:
        self.environment = os.environ.get("AMP_OSE_ENVIRONMENT")
        self.handler_name: str = handler_name
        self.name: str = name
        self._successor: Optional["CORHandler"] = successor

    @abstractmethod
    async def _perform_check(self) -> bool:
        """Abstract method to be implemented by subclasses to perform specific check.
        :return: True if the check passes, False otherwise.
        """
        pass

    async def handle_check(self) -> Union[bool, dict[str, Any]]:
        check_passed: bool = False
        return_details: dict[str, Any] = {}

        if not self.environment:
            logger.error("Environment variable 'AMP_OSE_ENVIRONMENT' not set, aborting operation!.")
            return False, return_details

        logger.info(f"CoR: Handling check for {self.handler_name}.")

        try:
            check_passed = await self._perform_check()
        except Exception as generic_exception:
            logger.error(f"Error inside handler [{self.handler_name}]: {repr(generic_exception)}")
            return_details[f"{self.handler_name}_{self.environment}"] = f"Error: {repr(generic_exception)}"
            return False, return_details

        if check_passed:
            logger.info(f"CoR: Check passed for {self.handler_name}.")
            return_details[f"{self.handler_name}_{self.environment}"] = "Passed"
            if self._successor:
                logger.info(f"CoR: Passing from {self.handler_name} to successor: {self._successor.handler_name}.")
                return await self._successor.handle_check()
            else:
                logger.info(f"CoR: End of chain reached for {self.name}, all prior checks passed.")
                return True, return_details
        else:
            logger.error(f"CoR: Check failed for {self.handler_name}.")
            return_details[f"{self.handler_name}_{self.environment}"] = "Failed"
            return False, return_details