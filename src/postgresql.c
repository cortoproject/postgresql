/* $CORTO_GENERATED
 *
 * postgresql.c
 *
 * Only code written between the begin and end tags will be preserved
 * when the file is regenerated.
 */

#include "corto/postgresql/postgresql.h"

/* $header() */
#include <libpq-fe.h>

corto_uint8 POSTGRESQL_DB_HANDLE;

static void postgresql_cleanup(void *userData) {
    PGconn *conn = userData;
    PQfinish(conn);
}
/* $end */

int postgresqlMain(int argc, char* argv[]) {
/* $begin(main) */

    POSTGRESQL_DB_HANDLE = corto_olsKey(postgresql_cleanup);

    return 0;
/* $end */
}
