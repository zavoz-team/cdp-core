from usecase.dto import GetHealthStatusResult


class HealthService:
    def __init__(self, service: str = 'cdp-core') -> None:
        self._service = service

    async def get_status(self) -> GetHealthStatusResult:
        return GetHealthStatusResult(service=self._service)
