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
        "CREATE TABLE IF NOT EXISTS corto ("\
            "path ltree unique,"\
            "type text,"\
            "value jsonb);"
    );

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        corto_seterr("cannot create corto table: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        goto error;
    }

    /* Register database connection with object */
    corto_olsSet(this, POSTGRESQL_DB_HANDLE, conn);

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
        "INSERT INTO corto(path, type, value) VALUES ('%s','%s','%s') ON CONFLICT DO NOTHING;",
        corto_path(path, root_o, observable, "."),
        corto_path(type, root_o, corto_typeof(observable), "/"),
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
        "DELETE FROM corto WHERE path = '%s';",
        corto_path(path, root_o, observable, ".")
    );

    printf("%s\n", stmt);

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

corto_resultIter _postgresql_Connector_onRequest(
    postgresql_Connector this,
    corto_string parent,
    corto_string expr,
    corto_string param,
    corto_bool setContent)
{
/* $begin(corto/postgresql/Connector/onRequest) */
    corto_iter result;
    memset(&result, 0, sizeof(result));
    return result;
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

    corto_asprintf(&stmt,
        "UPDATE corto SET value = '%s' WHERE path = '%s';",
        value,
        corto_path(path, root_o, observable, ".")
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
