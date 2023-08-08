# This script is set up to work with a single `users` table, and has code specific to the `users` use case (creating tables, and generating user events).  
# Shared Database parameters
USERS_TABLE_NAME = 'users'  # e.g. users. Should be lowercase to avoid issues with case sensitivity in different databases.

# Postgres connection details
PG_HOST_URL = ''  # e.g. postgres-cdc-demo.<myhost>.<myregion>.rds.amazonaws.com
PG_USERNAME = 'postgres'  # e.g. postgres (Postgres default)
PG_PASSWORD = ''  # e.g. MySecretPassword!
PG_DATABASE = ''  # e.g. postgres_cdc_demo   #  Note the use of _ in the database name compared to the host name as it's used for several services and _ is allowed across all as a separator.
PG_PORT = 5432  # e.g. 5432
# The script will use the Confluent API and deploy a Postgres CDC Source Connector with this name. 
PG_CONFLUENT_CONNECTOR_NAME = 'PostgresDbzmConnector_0'  # This is a simple friendly name, you can set it here for convenience. 

# MySQL connection details
MYSQL_HOST_URL = ''   # e.g. mysql-cdc-demo.<myhost>.<myregion>.rds.amazonaws.com
MYSQL_PORT = 3306  # e.g. 3306
MYSQL_DB_NAME = ''  # e.g. mysql_cdc_demo. Again note the use of _.
MYSQL_USERNAME = 'admin'  # e.g. admin
MYSQL_PASSWORD = ''  # e.g. MySecretPassword!
# The script will use the Confluent API and deploy a MySQL CDC Source Connector with this name. 
MYSQL_CONFLUENT_CONNECTOR_NAME = 'MysqlDbzmConnector_0'  # This is a simple friendly name, you can set it here for convenience.

# Confluent Cloud Cluster connection details
CONFLUENT_ENV_NAME = 'default'  # e.g. default. This is the name of the environment you created in Confluent Cloud, it defaults to 'default'.
CONFLUENT_CLUSTER_NAME = ''  # e.g. cluster_eu. This name will also be used in Tinybird as the connection name.
CONFLUENT_BOOTSTRAP_SERVERS = ''  # e.g. <clusterId>.<myregion>.<myprovider>.confluent.cloud:9092
CONFLUENT_UNAME = ''  # This is your Confluent Key from your authentication details for the cluster. Not to be confused with your Confluent Cloud API key.
CONFLUENT_SECRET = ''  # This is your Confluent Secret from your Auth details. Not to be confused with your Confluent Cloud API secret.
CONFLUENT_OFFSET_RESET = 'latest'  # e.g. latest. This is the offset to start reading from the topic. It defaults to latest.

# Confluent Cloud API connection details
CONFLUENT_CLOUD_KEY = ''  # This is your Confluent Cloud API key for your user account, not to be confused with API keys for your Kafka Cluster.
CONFLUENT_CLOUD_SECRET = ''  # This is your Confluent Cloud API secret for your user account, not to be confused with API keys for your Kafka Cluster.

# Tinybird connection details
TINYBIRD_API_URI = 'api'  # may also be api.us-east
TINYBIRD_API_KEY = ''  # This is your Tinybird API key. It should have rights to create datasources and pipes, so your default Admin token is easiest.
TINYBIRD_CONFLUENT_CONNECTION_NAME = 'cluster_eu_cdc_demo'  # Name that Tinybird uses for the Tinybird stream connection. 
