PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_CPPFLAGS =  -I/home/ildus/src/jansson-2.7/src
MODULE_big = decoder_json
OBJS = decoder_json.o /home/ildus/src/jansson-2.7/build/lib/libjansson.a

REGRESS = second

include $(PGXS)

deps:
	wget https://github.com/akheron/jansson/archive/v2.7.tar.gz
	tar xzf v2.7.tar.gz

test:
	- psql contrib_regression -c "SELECT pg_drop_replication_slot('custom_slot');"
	- make clean
	make
	rm `pg_config --pkglibdir`/decoder_json.so
	cp decoder_json.so `pg_config --pkglibdir`/
	make installcheck