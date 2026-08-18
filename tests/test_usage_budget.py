from types import SimpleNamespace

import pytest

from kairos_persistence.usage_budget import LLM_BUDGET_SERVICE, DurableLLMUsageBudget


class _Runtime:
    def __init__(self):
        self.database = SimpleNamespace(pool=object())
        self.started = 0

    async def start(self):
        self.started += 1


class _Repository:
    def __init__(self):
        self.reservations = []
        self.commits = []

    async def reserve_usage(self, **kwargs):
        self.reservations.append(kwargs)

    async def commit_usage(self, *args):
        self.commits.append(args)


@pytest.mark.asyncio
async def test_llm_budget_maps_each_microdollar_to_one_durable_usage_unit():
    runtime = _Runtime()
    repository = _Repository()
    observed_pools = []

    def factory(pool):
        observed_pools.append(pool)
        return repository

    budget = DurableLLMUsageBudget(runtime, repository_factory=factory)
    await budget.reserve(
        provider="openai",
        reservation_id="kairos-llm-v1:openai:request",
        reserved_microusd=123_456,
        monthly_budget_microusd=45_000_000,
    )
    await budget.commit(
        provider="openai",
        reservation_id="kairos-llm-v1:openai:request",
        actual_microusd=7_890,
    )

    assert runtime.started == 2
    assert observed_pools == [runtime.database.pool, runtime.database.pool]
    assert repository.reservations == [
        {
            "service": LLM_BUDGET_SERVICE,
            "source": "openai",
            "reservation_id": "kairos-llm-v1:openai:request",
            "reserved_units": 123_456,
            "unit_cost_microusd": 1,
            "monthly_budget_microusd": 45_000_000,
        }
    ]
    assert repository.commits == [(LLM_BUDGET_SERVICE, "openai", "kairos-llm-v1:openai:request", 7_890)]
