"""Provider-wide durable microdollar reservations for paid runtime calls."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from .runtime import DurableMessageBus
from .source_state import SourceStateRepository

LLM_BUDGET_SERVICE = "kairos-llm-v1"


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


class DurableLLMUsageBudget:
    """Adapt the generic usage ledger to the ``kairos-llm`` budget protocol.

    Every runtime service uses the same service/source identity, so concurrent
    Text, Aggregator and Macro reservations share one provider-wide monthly cap.
    One reserved unit equals one microdollar.
    """

    def __init__(
        self,
        runtime: DurableMessageBus,
        *,
        repository_factory: Callable[[Any], _CostRepository] = SourceStateRepository,
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
