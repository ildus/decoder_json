/*-------------------------------------------------------------------------
 *
 * decoder_json.c
 *      Logical decoding output plugin generating JSON structures based
 *      on things decoded.
 *
 * Author, Ildus Kurbangaliev
 *
 * IDENTIFICATION
 *        decoder_json/decoder_json.c
 *
 * TODO:
 * 1) add detail logs for unsupported types and other cases (replica identity)
 * 2) use postgresql internal realization of json
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/genam.h"
#include "access/sysattr.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "nodes/parsenodes.h"
#include "replication/output_plugin.h"
#include "replication/logical.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/array.h"
#include <jansson.h>

PG_MODULE_MAGIC;

/* These must be available to pg_dlsym() */
extern void     _PG_init(void);
extern void     _PG_output_plugin_init(OutputPluginCallbacks *cb);

/*
 * Structure storing the plugin specifications and options.
 */
typedef struct
{
    MemoryContext context;
    bool include_transaction;
    bool sort_keys;
} DecoderRawData;

typedef struct
{
    char *name;
    json_t *value;
} Pair;

static void decoder_json_startup(LogicalDecodingContext *ctx,
                                OutputPluginOptions *opt,
                                bool is_init);
static void decoder_json_shutdown(LogicalDecodingContext *ctx);
static void decoder_json_begin_txn(LogicalDecodingContext *ctx,
                                  ReorderBufferTXN *txn);
static void decoder_json_commit_txn(LogicalDecodingContext *ctx,
                                   ReorderBufferTXN *txn,
                                   XLogRecPtr commit_lsn);
static void decoder_json_change(LogicalDecodingContext *ctx,
                               ReorderBufferTXN *txn, Relation rel,
                               ReorderBufferChange *change);

void
_PG_init(void)
{
    /* other plugins can perform things here */
}

/* specify output plugin callbacks */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
    AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

    cb->startup_cb = decoder_json_startup;
    cb->begin_cb = decoder_json_begin_txn;
    cb->change_cb = decoder_json_change;
    cb->commit_cb = decoder_json_commit_txn;
    cb->shutdown_cb = decoder_json_shutdown;
}


/* initialize this plugin */
static void
decoder_json_startup(LogicalDecodingContext *ctx,
                     OutputPluginOptions *opt,
                     bool is_init)
{
    ListCell   *option;
    DecoderRawData *data;

    data = palloc(sizeof(DecoderRawData));
    data->context = AllocSetContextCreate(ctx->context,
                                          "Raw decoder context",
                                          ALLOCSET_DEFAULT_MINSIZE,
                                          ALLOCSET_DEFAULT_INITSIZE,
                                          ALLOCSET_DEFAULT_MAXSIZE);
    data->include_transaction = false;
    data->sort_keys = false;

    ctx->output_plugin_private = data;

    /* Default output format */
    opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;

    foreach(option, ctx->output_plugin_options)
    {
        DefElem    *elem = lfirst(option);

        Assert(elem->arg == NULL || IsA(elem->arg, String));

        if (strcmp(elem->defname, "include_transaction") == 0)
        {
            /* if option does not provide a value, it means its value is true */
            if (elem->arg == NULL)
                data->include_transaction = true;
            else if (!parse_bool(strVal(elem->arg), &data->include_transaction))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("could not parse value \"%s\" for parameter \"%s\"",
                                strVal(elem->arg), elem->defname)));
        }
        else if (strcmp(elem->defname, "sort_keys") == 0) {
            /* if option does not provide a value, it means its value is true */
            if (elem->arg == NULL)
                data->sort_keys = true;
            else if (!parse_bool(strVal(elem->arg), &data->sort_keys))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("could not parse value \"%s\" for parameter \"%s\"",
                                strVal(elem->arg), elem->defname)));
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("option \"%s\" = \"%s\" is unknown",
                            elem->defname,
                            elem->arg ? strVal(elem->arg) : "(null)")));
        }
    }
}

/* cleanup this plugin's resources */
static void
decoder_json_shutdown(LogicalDecodingContext *ctx)
{
    DecoderRawData *data = ctx->output_plugin_private;

    /* cleanup our own resources via memory context reset */
    MemoryContextDelete(data->context);
}

/* BEGIN callback */
static void
decoder_json_begin_txn(LogicalDecodingContext *ctx,
                       ReorderBufferTXN *txn)
{
    DecoderRawData *data = ctx->output_plugin_private;

    /* Write to the plugin only if there is */
    if (data->include_transaction)
    {
        OutputPluginPrepareWrite(ctx, true);
        appendStringInfoString(ctx->out, "begin");
        OutputPluginWrite(ctx, true);
    }
}

/* COMMIT callback */
static void
decoder_json_commit_txn(LogicalDecodingContext *ctx,
                        ReorderBufferTXN *txn,
                        XLogRecPtr commit_lsn)
{
    DecoderRawData *data = ctx->output_plugin_private;

    /* Write to the plugin only if there is */
    if (data->include_transaction)
    {
        OutputPluginPrepareWrite(ctx, true);
        appendStringInfoString(ctx->out, "commit");
        OutputPluginWrite(ctx, true);
    }
}

/*
 * Get a relation name.
 */
static char
*get_relname(Relation rel)
{
    Form_pg_class   class_form = RelationGetForm(rel);
    return quote_qualified_identifier(
                get_namespace_name(
                           get_rel_namespace(RelationGetRelid(rel))),
            NameStr(class_form->relname));
}

static json_t *get_json_value(Datum, Oid, bool);

static json_t
*get_json_array(Datum array, Oid elmtype, int elmlen, bool elmbyval) {
    Datum *elems;
    int nelems, i;
    ArrayType  *arr = DatumGetArrayTypeP(array);
    json_t *result = json_array();

    if (ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != elmtype) {
        elog(ERROR, "expected 1-D array");
        return NULL;
    }

    deconstruct_array(arr, elmtype, elmlen, elmbyval, 'i', &elems, NULL, &nelems);
    for (i = 0; i < nelems; ++i) {
        json_array_append_new(result, get_json_value(elems[i], elmtype, false));
    }

    pfree(elems);
    return result;
}

/*
 * Get json value for datum.
 */
static json_t
*get_json_value(Datum origval,
                Oid typid,
                bool isnull)
{
    Oid typoutput;
    bool typisvarlena;
    Datum val;

    /* Query output function */
    getTypeOutputInfo(typid, &typoutput, &typisvarlena);

    if (isnull)
        return json_null();
    else if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval))
        return NULL; //unchanged-toast-datum
    else if (!typisvarlena)
        val = origval;
    else
    {
        /* Definitely detoasted Datum */
        val = PointerGetDatum(PG_DETOAST_DATUM(origval));
    }
    switch (typid) {
        case BOOLOID:
            return json_boolean(DatumGetBool(val));
        case INT2OID:
            return json_integer(DatumGetInt16(val));
        case INT4OID:
            return json_integer(DatumGetInt32(val));
        case INT8OID:
        case OIDOID:
            return json_integer(DatumGetInt64(val));
        case FLOAT4OID:
            return json_real(DatumGetFloat4(val));
        case FLOAT8OID:
            return json_real(DatumGetFloat8(val));
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            return json_string(timestamptz_to_str(DatumGetTimestampTz(val)));
        case NUMERICOID:
            /* we send numeric as string for data safety */
        case CHAROID:
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
        case JSONOID:
        case XMLOID:
        case UUIDOID:
            return json_string(OidOutputFunctionCall(typoutput, val));
        case 1015:
            /* varchar array */
            return get_json_array(val, VARCHAROID, -1, false);
        case TEXTARRAYOID:
            return get_json_array(val, TEXTOID, -1, false);
        case INT2ARRAYOID:
            return get_json_array(val, INT2OID, 2, true);
        case INT4ARRAYOID:
            return get_json_array(val, INT4OID, 4, true);
        case FLOAT4ARRAYOID:
            return get_json_array(val, FLOAT4OID, 4, true);
        case OIDARRAYOID:
            return get_json_array(val, OIDOID, 8, true);
        default:
            elog(ERROR, "Type %s is not supported, oid=%d", format_type_be(typid), typid);
    }
    return NULL;
}

/*
 * Get attr:value pair
 */
static Pair
*get_pair(TupleDesc tupdesc,
          HeapTuple tuple,
          int natt)
{
    Form_pg_attribute   attr;
    Datum               origval;
    bool                isnull;
    Pair *pair;
    char *attname;
    json_t *val;

    attr = tupdesc->attrs[natt - 1];

    /* Skip dropped columns and system columns */
    if (attr->attisdropped || attr->attnum < 0)
        return NULL;

    /* Get attribute name */
    attname = NameStr(attr->attname);

    /* Get Datum from tuple */
    origval = fastgetattr(tuple, natt, tupdesc, &isnull);

    val = get_json_value(origval, attr->atttypid, isnull);
    //elog(LOG, "We got json val for: %s=%s", attname, json_dumps(val, JSON_ENCODE_ANY));
    if (val == NULL)
        return NULL;

    pair = palloc(sizeof(Pair));
    pair->value = val;
    pair->name = attname;
    return pair;
}

static json_t
*get_pairs(TupleDesc tupdesc,
           HeapTuple tuple)
{
    int natt;
    json_t *pairs = json_object();

    for (natt = 0; natt < tupdesc->natts; natt++) {
        Pair *pair = get_pair(tupdesc, tuple, natt + 1);
        if (pair == NULL)
            continue;

        json_object_set_new(pairs, pair->name, pair->value);
        pfree(pair);
    }
    return pairs;
}

/*
 * Generate a WHERE clause for UPDATE or DELETE.
 */
static json_t
*get_where_clause(Relation relation,
                  HeapTuple oldtuple,
                  HeapTuple newtuple)
{
    TupleDesc tupdesc = RelationGetDescr(relation);
    int natt;
    json_t *allClauses = json_object();

    Assert(relation->rd_rel->relreplident == REPLICA_IDENTITY_DEFAULT ||
           relation->rd_rel->relreplident == REPLICA_IDENTITY_FULL ||
           relation->rd_rel->relreplident == REPLICA_IDENTITY_INDEX);

    RelationGetIndexList(relation);
    /* Generate WHERE clause using new values of REPLICA IDENTITY */
    if (OidIsValid(relation->rd_replidindex))
    {
        Relation    indexRel;
        int         key;

        /* Use all the values associated with the index */
        indexRel = index_open(relation->rd_replidindex, ShareLock);
        for (key = 0; key < indexRel->rd_index->indnatts; key++)
        {
            int relattr = indexRel->rd_index->indkey.values[key];

            /*
             * For a relation having REPLICA IDENTITY set at DEFAULT
             * or INDEX, if one of the columns used for tuple selectivity
             * is changed, the old tuple data is not NULL and need to
             * be used for tuple selectivity. If no such columns are
             * updated, old tuple data is NULL.
             */
            Pair *pair = get_pair(tupdesc, oldtuple ? oldtuple : newtuple, relattr);
            if (pair == NULL)
                continue;
            json_object_set_new(allClauses, pair->name, pair->value);
            pfree(pair);
        }
        index_close(indexRel, NoLock);
        return allClauses;
    }

    /* We need absolutely some values for tuple selectivity now */
    Assert(oldtuple != NULL &&
           relation->rd_rel->relreplident == REPLICA_IDENTITY_FULL);

    /*
     * Fallback to default case, use of old values and print WHERE clause
     * using all the columns. This is actually the code path for FULL.
     */
    for (natt = 0; natt < tupdesc->natts; natt++) {
        Pair *pair = get_pair(tupdesc, oldtuple, natt + 1);
        if (pair == NULL)
            continue;

        json_object_set_new(allClauses, pair->name, pair->value);
        pfree(pair);
    }
    return allClauses;
}

static void
write_struct(StringInfo s,
             int actionId,
             Relation relation,
             json_t *clause,
             json_t *data,
             bool sort_keys)
{
    char *relname = get_relname(relation);
    char *result;
    size_t flags = JSON_COMPACT;
    json_t *action = json_object();

    if (sort_keys) flags |= JSON_SORT_KEYS;

    /* Generate struct */    
    json_object_set_new(action, "a", json_integer(actionId));
    json_object_set_new(action, "r", json_string(relname));
    if (clause != NULL)
        json_object_set(action, "c", clause);
    if (data != NULL)
        json_object_set(action, "d", data);

    result = json_dumps(action, flags);
    json_decref(action);

    elog(LOG, "Struct:%s", result);

    appendStringInfoString(s, result);
    free(result);
}

/*
 * Decode an INSERT entry
 */
static void
decoder_json_insert(StringInfo s,
                   Relation relation,
                   HeapTuple tuple,
                   bool sort_keys)
{
    TupleDesc tupdesc = RelationGetDescr(relation);
    json_t *data = get_pairs(tupdesc, tuple);
    write_struct(s, 0, relation, NULL, data, sort_keys);
    json_decref(data);
}

/*
 * Decode a DELETE entry
 * Append to output json structure like
 * {"a": 2, "r": "public.table_name", "c": "some_clause"}
 */
static void
decoder_json_delete(StringInfo s,
                    Relation relation,
                    HeapTuple tuple,
                    bool sort_keys)
{
    /*
     * Here the same tuple is used as old and new values, selectivity will
     * be properly reduced by relation uses DEFAULT or INDEX as REPLICA
     * IDENTITY.
     */
    json_t *clause = get_where_clause(relation, tuple, tuple);
    write_struct(s, 2, relation, clause, NULL, sort_keys);
    json_decref(clause);
}


/*
 * Decode an UPDATE entry
 */
static void
decoder_json_update(StringInfo s,
                   Relation relation,
                   HeapTuple oldtuple,
                   HeapTuple newtuple,
                   bool sort_keys)
{
    json_t *clause;
    json_t *data;
    TupleDesc tupdesc = RelationGetDescr(relation);

    /* If there are no new values, simply leave as there is nothing to do */
    if (newtuple == NULL)
        return;

    clause = get_where_clause(relation, oldtuple, newtuple);
    data = get_pairs(tupdesc, newtuple);
    write_struct(s, 1, relation, clause, data, sort_keys);
    json_decref(clause);
    json_decref(data);
}

/*
 * Callback for individual changed tuples
 */
static void
decoder_json_change(LogicalDecodingContext *ctx,
                    ReorderBufferTXN *txn,
                    Relation relation,
                    ReorderBufferChange *change)
{
    DecoderRawData *data;
    MemoryContext   old;
    char            replident = relation->rd_rel->relreplident;
    bool            is_rel_non_selective;

    data = ctx->output_plugin_private;

    /* Avoid leaking memory by using and resetting our own context */
    old = MemoryContextSwitchTo(data->context);

    /*
     * Determine if relation is selective enough for WHERE clause generation
     * in UPDATE and DELETE cases. A non-selective relation uses REPLICA
     * IDENTITY set as NOTHING, or DEFAULT without an available replica
     * identity index.
     */
    RelationGetIndexList(relation);
    is_rel_non_selective = (replident == REPLICA_IDENTITY_NOTHING ||
                            (replident == REPLICA_IDENTITY_DEFAULT &&
                             !OidIsValid(relation->rd_replidindex)));

    /* Decode entry depending on its type */
    switch (change->action)
    {
        case REORDER_BUFFER_CHANGE_INSERT:
            if (change->data.tp.newtuple != NULL)
            {
                OutputPluginPrepareWrite(ctx, true);
                decoder_json_insert(ctx->out,
                                   relation,
                                   &change->data.tp.newtuple->tuple,
                                   data->sort_keys);
                OutputPluginWrite(ctx, true);
            }
            break;
        case REORDER_BUFFER_CHANGE_UPDATE:
            if (!is_rel_non_selective)
            {
                HeapTuple oldtuple = change->data.tp.oldtuple != NULL ?
                    &change->data.tp.oldtuple->tuple : NULL;
                HeapTuple newtuple = change->data.tp.newtuple != NULL ?
                    &change->data.tp.newtuple->tuple : NULL;

                OutputPluginPrepareWrite(ctx, true);
                decoder_json_update(ctx->out,
                                   relation,
                                   oldtuple,
                                   newtuple,
                                   data->sort_keys);
                OutputPluginWrite(ctx, true);
            }
            break;
        case REORDER_BUFFER_CHANGE_DELETE:
            if (!is_rel_non_selective)
            {
                OutputPluginPrepareWrite(ctx, true);
                decoder_json_delete(ctx->out,
                                   relation,
                                   &change->data.tp.oldtuple->tuple,
                                   data->sort_keys);
                OutputPluginWrite(ctx, true);
            }
            break;
        default:
            /* Should not come here */
            Assert(0);
            break;
    }

    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);
}
