BEGIN;
CREATE TABLE IF NOT EXISTS lottery_draws (
  id      BIGSERIAL PRIMARY KEY,
  game    TEXT NOT NULL CHECK (game IN ('mini','loto6','loto7','bingo5','numbers3','numbers4')),
  round   INTEGER,
  ds      DATE NOT NULL,
  n1 INTEGER, n2 INTEGER, n3 INTEGER, n4 INTEGER, n5 INTEGER, n6 INTEGER, n7 INTEGER,
  b1 INTEGER, b2 INTEGER,
  raw_json JSONB
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_lottery_draws_game_round
  ON lottery_draws (game, COALESCE(round, -1), ds);
CREATE INDEX IF NOT EXISTS ix_lottery_draws_ds   ON lottery_draws (ds);
CREATE INDEX IF NOT EXISTS ix_lottery_draws_game ON lottery_draws (game);
COMMIT;
