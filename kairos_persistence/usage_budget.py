"""Provider-wide durable microdollar reservations for paid runtime calls."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from .runtime import DurableMessageBus
from .source_state import QUALIFICATION_CAMPAIGN_ID, SourceStateRepository

LLM_BUDGET_SERVICE = "kairos-llm-v1"


def _campaign_repository(pool: Any) -> SourceStateRepository:
    return SourceStateRepository(pool, campaign_id=QUALIFICATION_CAMPAIGN_ID)


class _CostRepository(Protocol):
    async def reserve_usage(
        self,
        *,
        service: str,
        source: str,
        reservation_id: str,
        reserved_units: int,
        unit_cost_microusd: int,
        monthly_budget_microusd: int,
    ) -> Any: ...

    async def commit_usage(
        self,
        service: str,
        source: str,
        reservation_id: str,
        actual_units: int,
    ) -> Any: ...


class CampaignLLMUsageBudget:
    """Standalone probe adapter using the same already-adopted provider ledger."""

    def __init__(self, repository: SourceStateRepository) -> None:
        if repository.campaign_id != QUALIFICATION_CAMPAIGN_ID:
            raise ValueError("LLM qualification requires the shared campaign identity")
        self.repository = repository

    async def reserve(
        self,
        *,
        provider: str,
        reservation_id: str,
        reserved_microusd: int,
        monthly_budget_microusd: int,
    ) -> None:
        await self.repository.reserve_usage(
            service=LLM_BUDGET_SERVICE,
            source=provider,
            reservation_id=reservation_id,
            reserved_units=reserved_microusd,
            unit_cost_microusd=1,
            monthly_budget_microusd=monthly_budget_microusd,
        )

    async def commit(self, *, provider: str, reservation_id: str, actual_microusd: int) -> None:
        await self.repository.commit_usage(LLM_BUDGET_SERVICE, provider, reservation_id, actual_microusd)


class DurableLLMUsageBudget:
    """Adapt the generic usage ledger to the ``kairos-llm`` budget protocol.

    Every runtime service uses the same service/source identity, so concurrent
    Text, Aggregator, Macro and probes share one cumulative qualification cap.
    An explicit reconciled historical adoption must exist before paid calls.
    One reserved unit equals one microdollar.
    """

    def __init__(
        self,
        runtime: DurableMessageBus,
        *,
        repository_factory: Callable[[Any], _CostRepository] = _campaign_repository,
    ) -> None:
        self.runtime = runtime
        self.repository_factory = repository_factory

    async def reserve(
        self,
        *,
        provider: str,
        reservation_id: str,
        reserved_microusd: int,
        monthly_budget_microusd: int,
    ) -> None:
        repository = await self._repository()
        await repository.reserve_usage(
            service=LLM_BUDGET_SERVICE,
            source=provider,
            reservation_id=reservation_id,
            reserved_units=reserved_microusd,
            unit_cost_microusd=1,
            monthly_budget_microusd=monthly_budget_microusd,
        )

    async def commit(
        self,
        *,
        provider: str,
        reservation_id: str,
        actual_microusd: int,
    ) -> None:
        repository = await self._repository()
        await repository.commit_usage(
            LLM_BUDGET_SERVICE,
            provider,
            reservation_id,
            actual_microusd,
        )

    async def _repository(self) -> _CostRepository:
        await self.runtime.start()
        return self.repository_factory(self.runtime.database.pool)
