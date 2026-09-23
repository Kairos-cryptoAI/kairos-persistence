"""Persist advisory LLM proposals from a bus into the isolated SIM ledger."""

from __future__ import annotations

import re

from kairos_core import LLMTradeProposalV1, Topics
from kairos_core.bus.base import BusEnvelope, MessageBus

from .simulator_proposals import SimulatorProposalRepository

SIMULATOR_PROPOSAL_CONSUMER_GROUP = "simulator-llm-proposal-ledger-v1"
_CONSUMER_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


async def consume_simulator_proposals(
    repository: SimulatorProposalRepository,
    bus: MessageBus,
    *,
    consumer: str,
) -> None:
    """Append the advisory proposal stream to SIM before acknowledging messages.

    The repository constructor requires a writable SIMULATOR database profile.
    This consumer accepts no strategy, risk, venue, or execution dependencies.
    A storage failure or malformed/foreign-topic message propagates without an
    ACK; an ACK failure is safe to replay because the repository is idempotent.
    The function is opt-in and never starts itself or opens a bus/database.
    """

    if not isinstance(repository, SimulatorProposalRepository):
        raise TypeError("proposal consumer requires SimulatorProposalRepository")
    if not isinstance(bus, MessageBus):
        raise TypeError("proposal consumer requires an injected MessageBus")
    if not isinstance(consumer, str) or not _CONSUMER_NAME.fullmatch(consumer):
        raise ValueError("consumer must be a normalized non-empty identifier")

    topic = Topics.LLM_TRADE_PROPOSAL
    async for envelope in bus.subscribe(
        topic,
        group=SIMULATOR_PROPOSAL_CONSUMER_GROUP,
        consumer=consumer,
    ):
        if not isinstance(envelope, BusEnvelope):
            raise TypeError("proposal bus yielded a non-BusEnvelope value")
        if envelope.topic != topic:
            raise ValueError("proposal bus yielded a message from a different topic")
        if type(envelope.payload) is not dict:
            raise TypeError("proposal message payload must be a JSON object")

        proposal = LLMTradeProposalV1.model_validate(envelope.payload)
        await repository.record(proposal)
        await bus.ack(topic, envelope, group=SIMULATOR_PROPOSAL_CONSUMER_GROUP)
