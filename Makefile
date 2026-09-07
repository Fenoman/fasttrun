# fasttrun - расширение для быстрых операций над временными таблицами
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
DATA = extension/fasttrun--2.0.sql \
       extension/fasttrun--2.1.sql \
       extension/fasttrun--2.1.1.sql \
       extension/fasttrun--2.1.2.sql \
       extension/fasttrun--2.2.0.sql \
       extension/fasttrun--2.3.0.sql \
       extension/fasttrun--2.3.1.sql \
       extension/fasttrun--2.3.2.sql \
       extension/fasttrun--2.3.3.sql \
       extension/fasttrun--2.3.4.sql \
       extension/fasttrun--2.4.0.sql \
       extension/fasttrun--2.4.1.sql \
       extension/fasttrun--2.0--2.1.sql \
       extension/fasttrun--2.1--2.1.1.sql \
       extension/fasttrun--2.1.1--2.1.2.sql \
       extension/fasttrun--2.1.2--2.2.0.sql \
       extension/fasttrun--2.2.0--2.3.0.sql \
       extension/fasttrun--2.3.0--2.3.1.sql \
       extension/fasttrun--2.3.1--2.3.2.sql \
       extension/fasttrun--2.3.2--2.3.3.sql \
       extension/fasttrun--2.3.3--2.3.4.sql \
       extension/fasttrun--2.3.4--2.4.0.sql \
       extension/fasttrun--2.4.0--2.4.1.sql \
       extension/fasttrun--unpackaged--2.0.sql
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
          fasttrun_plan_cache_survive \
          fasttrun_stats_width \
          fasttrun_discard \
          fasttrun_zero_sinval_catalog

PG_CONFIG ?= pg_config

# Статические проверки и pre-release driver сами не используют PGXS.
NO_PGXS_GOALS = check-docs check-required-suite check-prerelease
ifeq ($(strip $(MAKECMDGOALS)),)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else ifneq ($(strip $(filter-out $(NO_PGXS_GOALS),$(MAKECMDGOALS))),)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif

.PHONY: check-parity check-soak check-perf-smoke check-hook-chain \
        check-zero-sinval check-commit-inval-overhead \
        check-bulk-overhead check-on-commit-drop-leak \
        check-commit-duration check-replace-catalog check-giant-temp \
        check-xact-journal-memory check-cache-init-faults check-fault-matrix \
        check-tracking-order check-no-temp-impact check-planner-probes \
        check-publication-atomicity check-tracking-persistence \
        check-brin-stress check-required-suite check-prerelease \
        check-docs check-deep-local

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

check-commit-inval-overhead:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_commit_inval_overhead.sh

check-bulk-overhead:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_bulk_analyze_overhead.sh

check-on-commit-drop-leak:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_on_commit_drop_leak.sh

check-commit-duration:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_commit_duration.sh

check-replace-catalog:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_replace_analyze_catalog.sh

check-giant-temp:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_giant_temp.sh

check-xact-journal-memory:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_xact_journal_memory.sh

check-cache-init-faults:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_cache_init_faults.sh

check-brin-stress:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_brin_stress.sh

check-fault-matrix:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_fault_matrix.sh

check-tracking-order:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_tracking_order.sh

check-no-temp-impact:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_no_temp_impact.sh

check-planner-probes:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_planner_probes.sh all

check-publication-atomicity:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_publication_atomicity.sh

check-tracking-persistence:
	PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_tracking_persistence.sh

check-docs:
	python3 scripts/check_docs_consistency.py

check-required-suite:
	python3 scripts/check_required_suite.py

check-prerelease:
	scripts/check_fasttrun_prerelease.sh

check-deep-local: installcheck check-parity check-soak check-perf-smoke \
                  check-hook-chain check-zero-sinval \
                  check-commit-inval-overhead check-bulk-overhead \
                  check-on-commit-drop-leak check-commit-duration \
                  check-replace-catalog check-giant-temp \
                  check-no-temp-impact check-tracking-persistence \
                  check-required-suite check-docs
