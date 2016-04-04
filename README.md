# postgresql

postgresql connector for Corto

## Installation

This connector requires Postgres 9.5. This version is not yet available in Ubuntu repositories. To install on Ubuntu, see this page:
http://rayyoussef.com/2015/11/25/how-to-install-the-lastest-postgres-9-5-on-ubuntu/

To install postgres on OS X:

```
brew update
brew install postgres
```

Make sure you have the right version. This project has been tested on Postgres 9.5.2.

## Use

Once you have installed postgres, set up your database cluster and the database.

In the following examples, be careful to select the correct postgres version, i.e. check `which psql`. You may have to use the full path to your executable if there's more than one installation around e.g. `/usr/local/Cellar/postgresql/9.5.2/bin/psql`.

Set up the database cluster:

```
initdb "--pgdata=directory=$HOME/.corto/postgres"
```

Start the server:

```
pg_ctl -D "directory=$HOME/.corto/postgres" -l logfile start
```

Create the database:

```
createdb mydb
```

Create a scope that will be syncronized with the connector, and then create a connector on it:

```
void mydb

postgresql/Connector connector: mount=mydb mask=ON_SCOPE|ON_TREE name="mydb" table="cortoobjects" hostaddr="127.0.0.1"
```
