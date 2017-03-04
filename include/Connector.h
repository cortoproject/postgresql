/* Connector.h
 *
 * This file contains generated code. Do not modify!
 */

#ifndef CORTO_POSTGRESQL_CONNECTOR_H
#define CORTO_POSTGRESQL_CONNECTOR_H

#include <corto/corto.h>
#include <corto/postgresql/_interface.h>
#include <corto/postgresql/_type.h>
#include <corto/postgresql/_api.h>
#include <corto/postgresql/_meta.h>

#ifdef __cplusplus
extern "C" {
#endif


CORTO_POSTGRESQL_EXPORT corto_int16 _postgresql_Connector_construct(
    postgresql_Connector _this);
#define postgresql_Connector_construct(_this) _postgresql_Connector_construct(postgresql_Connector(_this))

CORTO_POSTGRESQL_EXPORT corto_void _postgresql_Connector_onDeclare(
    postgresql_Connector _this,
    corto_object observable);
#define postgresql_Connector_onDeclare(_this, observable) _postgresql_Connector_onDeclare(postgresql_Connector(_this), observable)

CORTO_POSTGRESQL_EXPORT corto_void _postgresql_Connector_onDelete(
    postgresql_Connector _this,
    corto_object observable);
#define postgresql_Connector_onDelete(_this, observable) _postgresql_Connector_onDelete(postgresql_Connector(_this), observable)

CORTO_POSTGRESQL_EXPORT corto_resultIter _postgresql_Connector_onRequest(
    postgresql_Connector _this,
    corto_request *request);
#define postgresql_Connector_onRequest(_this, request) _postgresql_Connector_onRequest(postgresql_Connector(_this), request)

CORTO_POSTGRESQL_EXPORT corto_object _postgresql_Connector_onResume(
    postgresql_Connector _this,
    corto_string parent,
    corto_string name,
    corto_object o);
#define postgresql_Connector_onResume(_this, parent, name, o) _postgresql_Connector_onResume(postgresql_Connector(_this), parent, name, o)

CORTO_POSTGRESQL_EXPORT corto_void _postgresql_Connector_onUpdate(
    postgresql_Connector _this,
    corto_object observable);
#define postgresql_Connector_onUpdate(_this, observable) _postgresql_Connector_onUpdate(postgresql_Connector(_this), observable)

#ifdef __cplusplus
}
#endif
#endif

