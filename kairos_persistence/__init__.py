"""Transactional audit trail and idempotency primitives for Kairos."""

from .canary_arm import PaperCanaryArm, PaperCanaryArmRepository
from .config import PersistenceSettings
from .database import Database
from .execution_journal import (
    EffectPreparation,
    EffectStatus,
    EffectType,
    ExecutionEffect,
    ExecutionJournalRepository,
)
from .mutation_budget import (
    DEFAULT_COMPENSATION_RESERVE,
    DEFAULT_MUTATION_CAPACITY,
    DEFAULT_MUTATION_WINDOW_MS,
    ExecutionMutationBudgetRepository,
    ExecutionMutationReservation,
)
from .offline_writer import OfflineDurableWriter, OfflineWriterError
from .repository import (
    AuditRepository,
    InboxClaim,
    InboxTransaction,
    MessageIdentityConflict,
    OutboxRecord,
)
from .runtime import DurableMessageBus, canonical_payload
from .runtime_health import ExecutionRuntimeHealth, ExecutionRuntimeHealthRepository
from .simulation_repository import (
    SimulationCommandCompletion,
    SimulationCommandPreparation,
    SimulationJournalRepository,
    SimulationRepository,
)
from .source_state import (
    CAMPAIGN_PROVIDER_CAPS,
    QUALIFICATION_CAMPAIGN_ID,
    CampaignSourceUsage,
    MonthlySourceUsage,
    SourceBudgetExceeded,
    SourceCursor,
    SourceStateRepository,
    UsageReservation,
    UsageStatus,
)
from .trade_lifecycle import (
    TERMINAL_TRADE_STATES,
    EquityState,
    NewTrade,
    OrderRole,
    RecoveryState,
    TradeLifecycleRepository,
    TradeMutationResult,
    TradeRecord,
    TradeState,
    validate_trade_transition,
)
from .usage_budget import LLM_BUDGET_SERVICE, CampaignLLMUsageBudget, DurableLLMUsageBudget

__all__ = [
    "CAMPAIGN_PROVIDER_CAPS",
    "QUALIFICATION_CAMPAIGN_ID",
    "CampaignSourceUsage",
    "CampaignLLMUsageBudget",
    "AuditRepository",
    "Database",
    "DurableMessageBus",
    "DurableLLMUsageBudget",
    "EffectStatus",
    "EffectPreparation",
    "EffectType",
    "ExecutionEffect",
    "ExecutionJournalRepository",
    "ExecutionRuntimeHealth",
    "ExecutionRuntimeHealthRepository",
    "SimulationRepository",
    "SimulationJournalRepository",
    "SimulationCommandPreparation",
    "SimulationCommandCompletion",
    "ExecutionMutationBudgetRepository",
    "ExecutionMutationReservation",
    "DEFAULT_COMPENSATION_RESERVE",
    "DEFAULT_MUTATION_CAPACITY",
    "DEFAULT_MUTATION_WINDOW_MS",
    "InboxClaim",
    "InboxTransaction",
    "MessageIdentityConflict",
    "OutboxRecord",
    "OfflineDurableWriter",
    "OfflineWriterError",
    "PaperCanaryArm",
    "PaperCanaryArmRepository",
    "PersistenceSettings",
    "MonthlySourceUsage",
    "SourceBudgetExceeded",
    "SourceCursor",
    "SourceStateRepository",
    "UsageReservation",
    "UsageStatus",
    "LLM_BUDGET_SERVICE",
    "canonical_payload",
    "EquityState",
    "NewTrade",
    "OrderRole",
    "RecoveryState",
    "TERMINAL_TRADE_STATES",
    "TradeLifecycleRepository",
    "TradeMutationResult",
    "TradeRecord",
    "TradeState",
    "validate_trade_transition",
]
