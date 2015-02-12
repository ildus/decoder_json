# decoder_json

A PostgreSQL logical decoder output plugin to deliver data in JSON

# Requirements:

* PostgreSQL 9.4+
* libjansson (for JSON support)

# Building

```
make deps
make
```

# Testing

Add to your postgresql.conf lines from logical.conf. Then run:

```
sudo chmod +x `pg_config --pkglibdir`
make test
```

# TODO:

Use built-in JSON from PostgreSQL for output generation.

# Useful links

* https://github.com/xstevens/decoderbufs - uses Protocol Buffers
* https://github.com/michaelpq/pg_plugins/tree/master/decoder_raw - generates SQL queries
* http://git.postgresql.org/gitweb/?p=postgresql.git;a=tree;f=contrib/test_decoding;h=ed74920f61b3440d2d880a8fdfc74ec8d12fcd35;hb=HEAD - sample decoder from postgresql sources