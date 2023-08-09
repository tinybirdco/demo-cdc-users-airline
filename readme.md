# CDC with Confluent & Debezium to Tinybird

#### Table of contents
* [Introduction](#cdc-intro)
  * [What the script does](#cdc-what-script-does)
* [Getting started](#getting-started)
  * [Setting up the demo script](#setting-up-code)
  * [Database server details](#databases)
    * [Using MySQL]()
    * [Using Postgres]()
  * [Confluent Cloud]()
  * [Tinybird]()
* [Using the demo script](#using-the-demo)
* [Things to know](#things-to-know)
* [Troubleshooting](#roubleshooting)

## [Introduction](#cdc-intro)

This repo walks through learning how to replicate a transactional table into Tinybird, so that all changes are captured in near real-time. Here we demonstrate a simple `users` database table that is receiving CREATE, UPDATE, and DELETE queries and streaming those Change Data Capture (CDC) events to Tinybird and an API endpoint that returns the up-to-the-second contents of the table. 

* The demo was written and tested with Postgres and MySQL, both hosted on AWS RDS. The demo should work with other cloud providers of Postgres and MySQL.
* Database table changes are captured using Confluent Cloud CDC Connectors, which are based on Debezium. This Connector streams the CDC events onto a Kafka Topic with a name based on the database schema and table name. This Connector can send the initial snapshot of the table along with change events.
* This Topic is then ingested into Tinybird in a raw format, and materialized into a replica of the current Users table where updates and deletes have been finalized.
* Once everything is configured (see below), the `demo_cdc.py` script handles creating the database table, creating the Debezium connection, creating Tinybird Data Sources, Pipes, and an API Endpoint, and generating CDC event by updating the soruce Database.
* This script was developed with Confluent Cloud and its APIs.  Any cloud provider with a Debezium-based connector should work with CDC processing, but this demo script exercises Confluent APIs for creating and deleting CDC connectors. 
* The combination of the source database (user) ``id`` column and a last updated timestamp ``updated_at`` shows how Tinybird can quickly and easily reconstruct the table.

### [What the script does](#cdc-what-script-does) 

* Ensures that a connection can be made to the Source database server. 
* Ensures a table with the configured name exists, and creates it if needed. 
* Generates a few initial events into the users table to be replicated.
* Creates the appropriate Postgres or MySQL Debezium-based Connector in Confluent Cloud.
* The Confluent Connector will create the Kafka Topic, snapshot the initial table state, and start replicating changes.
* Ensures that Tinybird has a Confluent connection to your Confluent Cloud cluster.
* Ingests the Topic into a 'raw' Tinybird Confluent Datasource containing all the user table change events.
* Creates a Tinybird Pipe which Materializes the users table with all changes in their final state.
* Publishes a simple Tinybird API endpoint to return users.
* You can re-run the demo script to generate further events to explore how they propagate through the pipeline.
* You can also run the script with another switch to remove the Pipeline.

## [Getting started](#getting-started)
You will need a Confluent Cloud account, an AWS Account, and a Tinybird Account, to work through this whole demo. You will need various authentication keys for these services to allow the automation to work. These configuration details are stored in the `conf.py` file, which includes descriptions and notes for your convenience. 

To get started with this demo, you need to bring the following:
* A Postgres or MySQL database on a server configured to generate CDC events.
* A Confluent Cloud account, with a cluster already created.
* A Tinybird Workspace created to ingest the CDC events. 

Now we can walk through deploying the infrastructure.

#### [Setting up the demo script](#setting-up-code)  

##### Python setup
You should use a virtualenv as good practice.
Once you have it set up, install the project requirements with 

```
pip install -r requirements.txt
```

##### Get the repo

Clone this repo locally, and get ready to put the required information into ``conf.py``. This includes account keys and secrets, database details, and stream cluster name.  

#### Database details

Confirm your database server is ready to generate CDC events. Depending on the database server type, and the CDC connector used, different configuration settings are required. 

##### Using Postgres

1. Access RDS via the AWS Console
2. Create Parameter group
Name / description / family: postgres14

3. Edit the param group, set these values:
rds.logical_replication = 1

4. Create the Database.
Postgres 14
Use the param group via additional options.
Make it publicly available in the network security group.

5. Get the connectivity information
Once the Database is deployed (it'll take a few minutes)
Grab the Postgres connection details and put them into conf.py

##### Using MySQL

Setting up MySQL follows much the same steps as Postgres above, with some variations detailed below.

When you create the parameter group in RDS, ensure that you do not select Aurora-Mysql, but plain MySQL (using 5.7 here)

The parameters to set for MySQL are:
binlog_format=ROW
binlog_row_image=full

You must enable Backups as well.

Grab the connectivity information and put it into conf.py

#### Confluent Cloud
If you don't already have a Confluent Cloud Environment and Kafka Cluster, you will need to create one.

Your cluster will need to be able to connect to AWS RDS, so either make it publicly accessible or ensure that you create the right security rules on both sides.

Likewise, Tinybird will need to be able to connect to Confluent, so ensure it is available to Tinybird as well.

#### Tinybird
If you don't already have a Tinybird Workspace, you will need to create one.

You can test if Tinybird can connect to your Confluent cluster by putting the connection details into the 'Add Datasource' interface in the UI - if it can connect it will retrieve a list of Topics on the cluster.


## [Using the demo script](#using-the-demo)

In these following commands you should submit either 'PG' or 'MYSQL' as your Database type with the ``--source-db`` command. 

If you don't supply a ``--source-db`` it will default to Postgres.

2. Test your connectivity
```
python demo_cdc.py --test-connection --source-db PG
```

3. Create the Pipeline
Run the demo script to generate the full pipelines so you can explore it.
```
python demo_cdc.py --create-pipeline --source-db PG
```

5. Generate events
You can generate more events to see how the Pipeline works, or test the latency etc. by running the script without other switches.
```
python demo_cdc.py --source-db PG
```

6. Remove the Pipeline
You can remove the pipeline again using the removal command, this can also be used to recreate it by combining the commands
```
python demo_cdc.py --remove-pipeline --source-db PG
```

## [Things to know](#things-to-know)

The Confluent connector for each Database type is automatically created for you, the process is quite simple and you can inspect it in the code. Note that the submission parameters are very speific and taken from the Confluent documentation; if you wish to experiment with other settings go right ahead but change and test carefully.

Once created, the Connectors and Topics will appear in your Confluent Cloud console as usual.

The Topic is automatically created with a single partition for simplicity.

If you want to create with multiple partitions for performance, you will also probably want to ensure your rows are partitioned by ID to ensure updates easy to sequentially process.

The script attempts to use publicly documented APIs or processes, just executed within a simple python environment. We try to minimise dependencies.
One notable choice is using the Tinybird definition files instead of raw API calls, as this mimics typical user CLI behavior and they are easier to read than raw python objects.

## [Troubleshooting](#troubleshooting)

If you recreate the Table and Confluent setup, you also have to recreate the Tinybird raw Data Source otherwise it'll still be using the existing consumer group and offsets.

The script assumes your CONFLUENT_CLOUD_KEY has the same rights as you would in the console, and is not a limited access service account.

If you are experimenting with other settings you may wish to create the Connector in the Confluent UI first, as it has more robust checking. The API sometimes simply gives a 500 if it doesn't like a field you have submitted, whereas the UI is a little more verbose.

