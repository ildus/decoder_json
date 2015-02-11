PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
MODULES = decoder_mf

include $(PGXS)