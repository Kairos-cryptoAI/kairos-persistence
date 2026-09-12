-- One outstanding technical session in this isolated project/database, not
-- one new allowance for every local or remote account. Existing conflicting
-- sessions intentionally make migration fail closed; never auto-retire them.
CREATE UNIQUE INDEX paper_canary_one_active_project
    ON paper_canary_sessions ((scope->>'project'))
    WHERE state IN ('ARMED','RUNNING','DRAINING');
