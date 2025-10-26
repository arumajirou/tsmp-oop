from __future__ import annotations
import os
from logging.config import fileConfig
from sqlalchemy import create_engine, pool
from alembic import context

# この env は ORM メタデータに依存しない（生SQL中心）
config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)

POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "sqlite:///ci.db")
target_metadata = None  # autogenerateは使わない

def run_migrations_offline():
    url = POSTGRES_DSN
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    connectable = create_engine(POSTGRES_DSN, poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
