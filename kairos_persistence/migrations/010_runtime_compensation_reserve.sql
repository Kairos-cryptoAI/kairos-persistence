ALTER TABLE execution_runtime_health
    ADD COLUMN IF NOT EXISTS local_mutation_compensation_reserve INTEGER NOT NULL DEFAULT 0;

ALTER TABLE execution_runtime_health
    DROP CONSTRAINT IF EXISTS execution_runtime_health_compensation_reserve_check;

ALTER TABLE execution_runtime_health
    ADD CONSTRAINT execution_runtime_health_compensation_reserve_check CHECK (
        local_mutation_compensation_reserve >= 0
        AND local_mutation_compensation_reserve <= local_mutation_capacity
    );
