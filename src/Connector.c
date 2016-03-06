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

    /* Construct replicator */
    return corto_replicator_construct(this);
error:
    return -1;
/* $end */
}

corto_void _postgresql_Connector_onDelete(
    postgresql_Connector this,
    corto_object observable)
{
/* $begin(corto/postgresql/Connector/onDelete) */

    /* << Insert implementation >> */

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

    printf("Update %s!\n", corto_nameof(observable));

/* $end */
}
