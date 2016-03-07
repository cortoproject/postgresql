/* $CORTO_GENERATED
 *
 * Connector.c
 *
 * Only code written between the begin and end tags will be preserved
 * when the file is regenerated.
 */

#include "corto/postgresql/postgresql.h"

/* $header() */
#include <libpq-fe.h>
extern corto_uint8 POSTGRESQL_DB_HANDLE;
/* $end */

corto_int16 _postgresql_Connector_construct(
    postgresql_Connector this)
{
/* $begin(corto/postgresql/Connector/construct) */
    char port[6];

    if (this->port) {
        sprintf(port, "%u", this->port);
    } else {
        sprintf(port, "5432"); /* Default port */
    }

    PGconn *conn = PQsetdbLogin(
        this->hostaddr ? this->hostaddr : "localhost",
        port,
        NULL,
        NULL,
        this->name,
        this->user,
        this->password
    );

    if (PQstatus(conn) != CONNECTION_OK) {
        corto_seterr("connection to database failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        goto error;
    }

    /* Set default to actual parameters */
    corto_setstr(&this->name, PQdb(conn));
    corto_setstr(&this->user, PQuser(conn));
    corto_setstr(&this->password, PQpass(conn));
    corto_setstr(&this->hostaddr, PQhost(conn));
    this->port = atoi(PQport(conn));

    /* Initialize database */
    PGresult *res = PQexec(conn,
        "CREATE EXTENSION IF NOT EXISTS ltree;"\
        "CREATE TABLE IF NOT EXISTS local ("\
            "path ltree unique,"\
            "type text,"\
            "value jsonb);"
    );

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        corto_seterr("cannot create 'local' table: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        goto error;
    }

    /* Register database connection with object */
    corto_olsSet(this, POSTGRESQL_DB_HANDLE, conn);

    corto_replicator_setContentType(this, "text/json");
    corto_replicator(this)->kind = CORTO_SINK;

    /* Construct replicator */
    return corto_replicator_construct(this);
error:
    return -1;
/* $end */
}

corto_void _postgresql_Connector_onDeclare(
    postgresql_Connector this,
    corto_object observable)
{
/* $begin(corto/postgresql/Connector/onDeclare) */
    PGconn *conn = corto_olsGet(this, POSTGRESQL_DB_HANDLE);
    corto_string stmt;
    corto_string value;
    corto_id path, type;

    value = json_fromCorto(observable);

    corto_asprintf(&stmt,
        "INSERT INTO local (path, type, value) VALUES ('root.%s','%s','%s') ON CONFLICT DO NOTHING;",
        corto_path(path, corto_replicator(this)->mount, observable, "."),
        corto_fullpath(type, corto_typeof(observable)),
        value
    );

    PGresult *res = PQexec(conn, stmt);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        corto_error("%s: %s",
          corto_fullpath(NULL, observable),
          PQerrorMessage(conn));
        PQclear(res);
    }

    corto_dealloc(value);
    corto_dealloc(stmt);

/* $end */
}

corto_void _postgresql_Connector_onDelete(
    postgresql_Connector this,
    corto_object observable)
{
/* $begin(corto/postgresql/Connector/onDelete) */
    PGconn *conn = corto_olsGet(this, POSTGRESQL_DB_HANDLE);
    corto_string stmt;
    corto_string value;
    corto_id path;

    value = json_fromCorto(observable);

    corto_asprintf(&stmt,
        "DELETE FROM local WHERE path = 'root.%s';",
        corto_path(path, corto_replicator(this)->mount, observable, ".")
    );

    PGresult *res = PQexec(conn, stmt);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        corto_error("%s: %s",
          corto_fullpath(NULL, observable),
          PQerrorMessage(conn));
        PQclear(res);
    }

    corto_dealloc(value);
    corto_dealloc(stmt);

/* $end */
}

/* $header(corto/postgresql/Connector/onRequest) */
typedef struct postgresql_iterData {
    PGresult *res;
    corto_int32 i;
    corto_result result;
} postgresql_iterData;

void* postgresql_iterNext(corto_iter *iter) {
    postgresql_iterData *data = iter->udata;
    data->result.id = PQgetvalue(data->res, data->i, 0);
    data->result.name = NULL;
    data->result.type = PQgetvalue(data->res, data->i, 2);
    data->result.parent = ".";
    data->result.value = 0;
    if (PQnfields(data->res) > 3) {
        data->result.value =
          (corto_word)corto_strdup(PQgetvalue(data->res, data->i, 3));
    }
    data->i ++;
    return &data->result;
}

int postgresql_iterHasNext(corto_iter *iter) {
    postgresql_iterData *data = iter->udata;
    return data->i < PQntuples(data->res);
}

void postgresql_iterRelease(corto_iter *iter) {
    postgresql_iterData *data = iter->udata;
    PQclear(data->res);
}
/* $end */
corto_resultIter _postgresql_Connector_onRequest(
    postgresql_Connector this,
    corto_string parent,
    corto_string expr,
    corto_string param,
    corto_bool setContent)
{
/* $begin(corto/postgresql/Connector/onRequest) */
    PGconn *conn = corto_olsGet(this, POSTGRESQL_DB_HANDLE);
    corto_string stmt;
    corto_id path;
    corto_resultIter result;

    /* In the 'local' table, path expressions start with 'root' */
    strcpy(path, "root/");
    strcat(path, parent);
    corto_cleanpath(path);

    /* The scope separator in postgres is a '.' */
    char *ptr = path, ch;
    while ((ch = *ptr)) {
        if (ch == '/') *ptr = '.';
        ptr++;
    }

    corto_asprintf(&stmt,
        "SELECT subpath(path, nlevel(path)-1, nlevel(path)) AS name,"
          "subpath(path, 0, nlevel(path)-1) AS parent,"
          "type "
          "%s"
        "FROM local "
        "WHERE subpath(path, 0, nlevel(path) - 1) ~ '%s';",
        setContent ? ", value " : "",
        path);

    PGresult *res = PQexec(conn, stmt);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        corto_error("SELECT failed: '%s'\n", PQerrorMessage(conn));
        PQclear(res);
        memset(&result, 0, sizeof(corto_iter));
    } else {
        postgresql_iterData *data = corto_calloc(sizeof(postgresql_iterData));
        data->res = res;
        data->i = 0;
        result.udata = data;
        result.hasNext = postgresql_iterHasNext;
        result.next = postgresql_iterNext;
        result.release = postgresql_iterRelease;
    }

    corto_dealloc(stmt);

    return result;
/* $end */
}

corto_object _postgresql_Connector_onResume(
    postgresql_Connector this,
    corto_string parent,
    corto_string name,
    corto_object o)
{
/* $begin(corto/postgresql/Connector/onResume) */
    PGconn *conn = corto_olsGet(this, POSTGRESQL_DB_HANDLE);
    corto_string stmt;
    corto_id path;

    sprintf(path, "%s/%s", parent, name);
    corto_cleanpath(path);

    char *ptr = path, ch;
    while ((ch = *ptr)) {
        if (ch == '/') *ptr = '.';
        ptr++;
    }

    corto_asprintf(&stmt,
        "SELECT value FROM local WHERE path = 'root.%s';", path);

    PGresult *res = PQexec(conn, stmt);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        printf("not found: '%s'\n", PQerrorMessage(conn));
        /* If record is not found, the database contains no persistent copy */
        PQclear(res);
    } else {
        /* Expect one field, one row */
        corto_string json;
        if ((PQnfields(res) == 1) && (PQntuples(res) == 1)) {
            json = PQgetvalue(res, 0, 0);
            if (!o) {
                
            }

            if (json_toCorto(o, json)) {
                corto_error("failed to deserialize '%s': %s",
                  json, corto_lasterr());
            }
        }
        PQclear(res);
    }

    return o;
/* $end */
}

corto_void _postgresql_Connector_onUpdate(
    postgresql_Connector this,
    corto_object observable)
{
/* $begin(corto/postgresql/Connector/onUpdate) */
    PGconn *conn = corto_olsGet(this, POSTGRESQL_DB_HANDLE);
    corto_string stmt;
    corto_string value;
    corto_id path;

    value = json_fromCorto(observable);

    corto_path(path, corto_replicator(this)->mount, observable, ".");

    corto_asprintf(&stmt,
        "UPDATE local SET value = '%s' WHERE path = 'root.%s';",
        value,
        path
    );

    PGresult *res = PQexec(conn, stmt);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        corto_error("%s: %s",
          corto_fullpath(NULL, observable),
          PQerrorMessage(conn));
        PQclear(res);
    }

    corto_dealloc(value);
    corto_dealloc(stmt);

/* $end */
}
