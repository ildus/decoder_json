PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
MODULE_big = decoder_json
OBJS = decoder_json.o
JANSSON_VERSION = 2.7
SHLIB_LINK = ./jansson-${JANSSON_VERSION}/build/lib/libjansson.a
PG_CPPFLAGS =  -I./jansson-${JANSSON_VERSION}/build/include
REGRESS = default

include $(PGXS)

deps:
	wget https://github.com/akheron/jansson/archive/v${JANSSON_VERSION}.tar.gz
	mv v${JANSSON_VERSION}.tar.gz jannson.tar.gz
	tar xzf jannson.tar.gz
	rm jannson.tar.gz
	echo "libjansson_a_CFLAGS = -fPIC" >> jansson-${JANSSON_VERSION}/src/Makefile.am
	cd jansson-${JANSSON_VERSION} && mkdir build && cd build && cmake .. && make

test:
	- psql contrib_regression -c "SELECT pg_drop_replication_slot('custom_slot');"
	rm `pg_config --pkglibdir`/decoder_json.so
	cp decoder_json.so `pg_config --pkglibdir`/
	make installcheck