-- jsonb の config->'config' に GIN を貼り、将来の検索/絞り込みに備える
CREATE INDEX IF NOT EXISTS idx_runs_cfg_gin
ON runs
USING gin ((config->'config'));
