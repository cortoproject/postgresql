/* $CORTO_GENERATED
 *
 * Connector.c
 *
 * Only code written between the begin and end tags will be preserved
 * when the file is regenerated.
 */

#include <corto/postgresql/postgresql.h>

/* $header() */
#include <libpq-fe.h>
extern corto_uint8 POSTGRESQL_DB_HANDLE;
/* $end */

corto_int16 _postgresql_Connector_construct(
    postgresql_Connector this)
{
/* $begin(corto/postgresql/Connector/construct) */
    char port[6];

    if (corto_mount_setContentType(this, "text/json")) {
        goto error;
    }

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
    corto_ptr_setstr(&this->name, PQdb(conn));
    corto_ptr_setstr(&this->user, PQuser(conn));
    corto_ptr_setstr(&this->password, PQpass(conn));
    corto_ptr_setstr(&this->hostaddr, PQhost(conn));
    if (!this->table) {
        corto_ptr_setstr(&this->table, "local");
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

    corto_mount(this)->kind = CORTO_SINK;

    /* Construct mount */
    return corto_mount_construct(this);
error:
    return -1;
/* $end */
}

corto_void _postgresql_Connector_onNotify(
    postgresql_Connector this,
    corto_eventMask event,
    corto_result *object)
{
/* $begin(corto/postgresql/Connector/onNotify) */
    PGconn *conn = corto_olsGet(this, POSTGRESQL_DB_HANDLE);

    corto_string stmt = 0;
    corto_id path;

    sprintf(path, "%s/%s", object->parent, object->id);
    corto_cleanpath(path, path);

    corto_trace("postgresql: notify: event = %d, path = %s, type = %s",
        event, path, object->type);

    switch (event) {
    case CORTO_ON_DEFINE:
        corto_asprintf(
            &stmt,
            "INSERT INTO %s (path, type, value) VALUES ('root.%s','%s','%s') ON CONFLICT DO NOTHING;",
            this->table,
            path,
            object->type,
            corto_result_getText(object)
        );
        break;
    case CORTO_ON_UPDATE:
        corto_asprintf(
            &stmt,
            "UPDATE %s SET value = '%s' WHERE path = 'root.%s';",
            this->table,
            corto_result_getText(object),
            path
        );
        break;
    case CORTO_ON_DELETE:
        corto_asprintf(
            &stmt,
            "DELETE FROM %s WHERE path = 'root.%s';",
            this->table,
            path
        );
        break;
    }

    if (!stmt) {
        goto finish;
    }

    corto_trace("postgresql: exec %s", stmt);
    PGresult *res = PQexec(conn, stmt);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        corto_error(
            "%s: %s",
            path,
            PQerrorMessage(conn)
        );
        PQclear(res);
    }

    corto_dealloc(stmt);

finish:;
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
    corto_trace("postgresql: query returned '%s/%s'", data->result.parent, data->result.id)
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
    corto_cleanpath(path, path);

    corto_trace("postgresql: request (parent = '%s', expr = '%s')",
      request->parent, request->expr);

    /* The scope separator in postgres is a '.' */
    char *ptr = path, ch;
    while ((ch = *ptr)) {
        if (ch == '/') *ptr = '.';
        ptr++;
    }

    if (!strcmp(request->expr, "*")) {
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
    } else {
        corto_asprintf(&stmt,
            "SELECT subpath(path, nlevel(path)-1, nlevel(path)) AS name,"
              "subpath(path, 0, nlevel(path)-1) AS parent,"
              "type "
              "%s"
            "FROM %s "
            "WHERE subpath(path, 0, nlevel(path) - 1) ~ '%s' "
              "AND subpath(path, nlevel(path)-1, nlevel(path)) = '%s';",
            request->content ? ", value " : "",
            this->table,
            path,
            request->expr);
    }

    corto_trace("postgresql: exec %s", stmt);

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
    corto_bool resumed = FALSE;

    sprintf(path, "%s/%s", parent, name);
    corto_cleanpath(path, path);

    char *ptr = path, ch;
    while ((ch = *ptr)) {
        if (ch == '/') *ptr = '.';
        ptr++;
    }

    corto_asprintf(&stmt,
      "SELECT value, type FROM %s WHERE path = 'root.%s';", this->table, path);

    corto_trace("postgresql: resume: exec %s", stmt);

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

            resumed = TRUE;

            corto_trace("postgresql: resuming: %s", json);

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

            /* Deserialize JSON into object */
            if (o && strcmp(json, "null")) {
                corto_value v = corto_value_object(o, NULL);
                if (json_toValue(&v, json)) {
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

    return resumed ? o : NULL;
/* $end */
}
