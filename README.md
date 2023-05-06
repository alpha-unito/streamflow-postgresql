# PostgreSQL Plugin for StreamFlow

## Installation

Simply install the package directory from [PyPI](https://pypi.org/project/streamflow-postgresql/) using [pip](https://pip.pypa.io/en/stable/). StreamFlow will automatically recognise it as a plugin and load it at each workflow execution.

```bash
pip install streamflow-postgresql
```

If everything worked correctly, whenever a workflow execution start the following message should be printed in the log:

```bash
Successfully registered plugin postgresql.plugin.PostgreSQLStreamFlowPlugin
```

## Usage

This plugin registers a new `Database` component, which extends the StreamFlow `CachedDatabase` class. To declare it, put the following lines inside a `streamflow.yml` configuration file.

```yaml
database:
  type: postgresql
  config:
    dbname: <dbname>               # The name of the database to use
    hostname: <hostname>           # The database hostname or IP address
    password: <password>           # Password to use when connecting to the database
    username: <username>           # Username to use when connecting to the database
```
