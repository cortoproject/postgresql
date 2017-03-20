/* $CORTO_GENERATED
 *
 * ConnectorTest.c
 *
 * Only code written between the begin and end tags will be preserved
 * when the file is regenerated.
 */

#include <include/test.h>

corto_void _test_ConnectorTest_setUp(
    test_ConnectorTest this)
{
/* $begin(test/ConnectorTest/setUp) */
    corto_object users = corto_lookup(NULL, "/test/users");
    test_assert(users);
    corto_setref(&this->userScope, users); // TODO check error
    test_assert(this->userScope != NULL);

    corto_object connector = postgresql_ConnectorCreateChild(
        NULL, "testConnector", this->userScope, CORTO_ON_SCOPE, "postgres",
        "Users", "localhost", 5432, "postgres", "password"
    );
    corto_setref(&this->connector, connector); // TODO check error

    corto_release(this->userScope);
    test_assert(this->connector != NULL);
/* $end */
}

corto_void _test_ConnectorTest_tc_saveUser(
    test_ConnectorTest this)
{
/* $begin(test/ConnectorTest/tc_saveUser) */
    test_User *user = test_UserCreateChild(this->userScope, "userTestSaveUser", "Bob", 40);
    test_assert(user != NULL);

    corto_iter it;
    corto_int16 ret = corto_select("/users", "userTestSaveUser").iter(&it);
    test_assert(ret == 0);

    test_assert(corto_iterHasNext(&it));
    corto_result *r = corto_iterNext(&it);
    test_assertstr(r->id, "userTestSaveUser");
    test_assertstr(r->parent, ".");
    test_assertstr(r->type, "/User");
    test_assert(!corto_iterHasNext(&it));
/* $end */
}

corto_void _test_ConnectorTest_tearDown(
    test_ConnectorTest this)
{
/* $begin(test/ConnectorTest/tearDown) */
/* $end */
}
