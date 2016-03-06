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
        "DELETE FROM local WHERE path = 'root.%s';",
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

corto_resultIter _postgresql_Connector_onRequest(
    postgresql_Connector this,
    corto_string parent,
    corto_string expr,
    corto_string param,
    corto_bool setContent)
{
/* $begin(corto/postgresql/Connector/onRequest) */
    corto_resultList list = corto_llNew();
    PGconn *conn = corto_olsGet(this, POSTGRESQL_DB_HANDLE);
    corto_string stmt;
    corto_id path;

    strcpy(path, "root/");
    strcat(path, parent);
    corto_cleanpath(path);

    char *ptr = path, ch;
    while ((ch = *ptr)) {
        if (ch == '/') *ptr = '.';
        ptr++;
    }

    if (!setContent) {
        corto_asprintf(&stmt,
          "SELECT path, type FROM local WHERE subpath(path, 0, nlevel(path) - 1) ~ '%s';",
          path);
    } else {
        corto_asprintf(&stmt,
          "SELECT path, type, value FROM local WHERE subpath(path, 0, nlevel(path) - 1) ~ '%s';",
          path);
    }

    PGresult *res = PQexec(conn, stmt);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        corto_error("SELECT failed: '%s'\n", PQerrorMessage(conn));
        PQclear(res);
    } else {
        /* Expect one field, one row */
        corto_int32 i, records = PQntuples(res);

        for (i = 0; i < records; i++) {
          corto_string dbpath = PQgetvalue(res, i, 0);
          corto_string type = PQgetvalue(res, i, 1);
          corto_string content = NULL;
          if (PQnfields(res) > 2) {
              content = PQgetvalue(res, i, 2);
          }

          char *pathArray[CORTO_MAX_SCOPE_DEPTH];
          corto_id path;
          strcpy(path, dbpath);
          corto_int32 count = corto_pathToArray(path, pathArray, ".");

          corto_resultSet(
              corto_resultListAppendAlloc(list),
              corto_strdup(pathArray[count - 1]),   /* Id */
              NULL, /* Name is same as id */
              ".", /* Parent relative to query */
              type,   /* Type */
              (corto_word)(content ? corto_strdup(content) : NULL)
          );
        }

        PQclear(res);
    }

    corto_dealloc(stmt);

    return corto_llIterAlloc(list);
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
        "SELECT value FROM local WHERE path = '%s';", path);

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
            if (json_toCorto(o, json)) {
                corto_error("failed to deserialize '%s': %s",
                  json, corto_lasterr());
            }
        }
        PQclear(res);
    }

    return NULL;
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
        "UPDATE local SET value = '%s' WHERE path = 'root.%s';",
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
