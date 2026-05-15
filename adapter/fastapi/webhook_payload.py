from datetime import datetime
from decimal import Decimal

import httpx
from pydantic import BaseModel

from domain.export_job import ActivationDestination
from usecase.dto import OutboundWebhookDeliveryResult, SegmentExportPayload
from usecase.interface import OutboundWebhookGateway


class WebhookMemberStats(BaseModel):
    orders_count: int
    total_revenue: Decimal
    currency: str


class WebhookMember(BaseModel):
    customer_id: str
    emails: list[str]
    phones: list[str]
    external_user_ids: list[str]
    segments: list[str]
    stats: WebhookMemberStats


class WebhookPayload(BaseModel):
    job_id: str
    segment_id: str
    exported_at: datetime
    members_count: int
    members: list[WebhookMember]

    @classmethod
    def from_dto(cls, payload: SegmentExportPayload) -> 'WebhookPayload':
        members = []
        for profile in payload.members:
            stats = WebhookMemberStats(
                orders_count=profile.orders_count,
                total_revenue=profile.total_revenue,
                currency=profile.currency,
            )
            member = WebhookMember(
                customer_id=profile.customer_id,
                emails=list(profile.identifiers.emails),
                phones=list(profile.identifiers.phones),
                external_user_ids=list(profile.identifiers.external_user_ids),
                segments=[str(s) for s in profile.current_segments],
                stats=stats,
            )
            members.append(member)

        return cls(
            job_id=payload.job_id,
            segment_id=str(payload.segment_id),
            exported_at=payload.exported_at,
            members_count=payload.members_count,
            members=members,
        )


class HttpxWebhookGateway(OutboundWebhookGateway):
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def send_segment_export(
        self,
        destination: ActivationDestination,
        payload: SegmentExportPayload,
    ) -> OutboundWebhookDeliveryResult:
        webhook_payload = WebhookPayload.from_dto(payload)
        json_data = webhook_payload.model_dump(mode='json')

        try:
            response = await self._client.post(
                destination.url,
                json=json_data,
                timeout=10.0,
            )

            if 200 <= response.status_code <= 299:
                return OutboundWebhookDeliveryResult.succeeded(
                    response_status_code=response.status_code
                )

            return OutboundWebhookDeliveryResult.failed(
                error_reason=f'HTTP {response.status_code}: {response.text[:100]}',
                response_status_code=response.status_code,
            )

        except httpx.TimeoutException:
            return OutboundWebhookDeliveryResult.failed(
                error_reason='Connection timeout'
            )
        except Exception as error:
            return OutboundWebhookDeliveryResult.failed(error_reason=str(error))
