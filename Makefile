# fasttrun — расширение для быстрых операций над временными таблицами
#                без генерации сообщений инвалидации общего кэша (sinval).
#
# Сборка против установленного PostgreSQL (использует pg_config из PATH):
#   make
#   make install
#   make installcheck
#
# Сборка против конкретной версии PostgreSQL:
#   make PG_CONFIG=/opt/homebrew/Cellar/postgresql@16/16.13/bin/pg_config
#
# Поддерживаемые версии PostgreSQL: 16, 17, 18.

MODULE_big = fasttrun
OBJS = fasttrun.o

EXTENSION = fasttrun
DATA = fasttrun--2.0.sql \
       fasttrun--2.1.sql \
       fasttrun--2.1.1.sql \
       fasttrun--2.1.2.sql \
       fasttrun--2.0--2.1.sql \
       fasttrun--2.1--2.1.1.sql \
       fasttrun--2.1.1--2.1.2.sql \
       fasttrun--unpackaged--2.0.sql
DOCS = README.md
PGFILEDESC = "fasttrun - sinval-free truncate and analyze for temporary tables"

REGRESS = fasttrun_basic \
          fasttrun_silent \
          fasttrun_stats_reset \
          fasttrun_analyze \
          fasttrun_migration \
          fasttrun_bench \
          fasttrun_stats \
          fasttrun_tracking \
          fasttrun_relstats_survive \
          fasttrun_plan_cache_survive

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: check-parity check-soak check-perf-smoke check-hook-chain check-zero-sinval check-deep-local

check-parity:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_analyze_parity.py --profile full
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_analyze_parity.py --profile default

check-soak:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_long_backend_soak.sh

check-perf-smoke:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_perf_smoke.sh

check-hook-chain:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_hook_chain.sh

check-zero-sinval:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_zero_shared_sinval.sh

check-deep-local: installcheck check-parity check-soak check-perf-smoke check-hook-chain check-zero-sinval
