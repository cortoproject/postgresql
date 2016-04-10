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

    if (!corto_checkAttr(this, CORTO_ATTR_SCOPED)) {
        corto_seterr("postgresql/Connector objects must be SCOPED");
        goto error;
    }

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
    if (!this->table) {
        corto_setstr(&this->table, "local");
    }
    this->port = atoi(PQport(conn));

    corto_string stmt;
    corto_asprintf(&stmt,
      "CREATE EXTENSION IF NOT EXISTS ltree;"\
      "CREATE TABLE IF NOT EXISTS %s ("\
          "path ltree unique,"\
          "type text,"\
          "value jsonb);", this->table);

    /* Initialize database */
    PGresult *res = PQexec(conn, stmt);
    corto_dealloc(stmt);

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        corto_seterr("cannot create '%s' table: %s", this->table, PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        goto error;
    }

    /* Register database connection with object */
    corto_olsSet(this, POSTGRESQL_DB_HANDLE, conn);

    corto_mount_setContentType(this, "text/json");
    corto_mount(this)->kind = CORTO_SINK;

    /* Construct mount */
    return corto_mount_construct(this);
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
        "INSERT INTO %s (path, type, value) VALUES ('root.%s','%s','%s') ON CONFLICT DO NOTHING;",
        this->table,
        corto_path(path, corto_mount(this)->mount, observable, "."),
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
    corto_id path;

    corto_asprintf(&stmt,
        "DELETE FROM %s WHERE path = 'root.%s';",
        this->table,
        corto_path(path, corto_mount(this)->mount, observable, ".")
    );

    PGresult *res = PQexec(conn, stmt);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        corto_error("%s: %s",
          corto_fullpath(NULL, observable),
          PQerrorMessage(conn));
        PQclear(res);
    }

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
    corto_request *request)
{
/* $begin(corto/postgresql/Connector/onRequest) */
    PGconn *conn = corto_olsGet(this, POSTGRESQL_DB_HANDLE);
    corto_string stmt;
    corto_id path;
    corto_resultIter result;

    /* In the 'local' table, path expressions start with 'root' */
    strcpy(path, "root/");
    strcat(path, request->parent);
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
        "FROM %s "
        "WHERE subpath(path, 0, nlevel(path) - 1) ~ '%s';",
        request->content ? ", value " : "",
        this->table,
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
      "SELECT value, type FROM %s WHERE path = 'root.%s';", this->table, path);

    PGresult *res = PQexec(conn, stmt);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        corto_error("resume failed: '%s'\n", PQerrorMessage(conn));
        /* If record is not found, the database contains no persistent copy */
        PQclear(res);
    } else {
        /* Expect two fields, one row */
        corto_string json;
        if ((PQnfields(res) == 2) && (PQntuples(res) == 1)) {
            corto_bool newObject = FALSE;
            json = PQgetvalue(res, 0, 0);

            /* If no object is passed to function, create one */
            if (!o) {
                corto_object parent_o =
                  corto_resolve(corto_mount(this)->mount, parent);
                if (parent) {
                    corto_object type_o =
                      corto_resolve(NULL, PQgetvalue(res, 0, 1));
                    if (type_o) {
                        o = corto_declareChild(parent_o, name, type_o);
                        if (!o) {
                            corto_seterr("failed to create object %s/%s: %s",
                              parent, name, corto_lasterr());
                        }
                        newObject = TRUE;
                        corto_release(type_o);
                    }
                    corto_release(parent_o);
                }
            }

            /* Deserialize JSON into objec */
            if (o) {
                if (json_toCorto(o, json)) {
                    corto_seterr("failed to deserialize '%s': %s",
                      json, corto_lasterr());
                } else if (newObject) {
                    if (corto_define(o)) {
                        corto_seterr("failed to define object %s/%s: %s",
                          parent, name, corto_lasterr());
                    }
                }
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

    corto_path(path, corto_mount(this)->mount, observable, ".");

    corto_asprintf(&stmt,
        "UPDATE %s SET value = '%s' WHERE path = 'root.%s';",
        this->table,
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
