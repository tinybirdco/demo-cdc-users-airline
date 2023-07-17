# CDC with Confluent & Debezium to Tinybird

This repo walks through learning how to replicate a transactional table into Tinybird, so that all changes are captured in near real-time.

* We provide the examples of a simple Users table that has inserts, updates and deletes.
* We provide examples of sourcing from both Postgres and MySQL, both hosted on AWS RDS.
* The Table is captured using the Confluent Cloud Debezium connector into a Kafka Topic containing both the initial snapshot and the change events.
* This is then ingested into Tinybird in a raw format, and then materialized into a replica of the original Users table where updates and deletes have been finalised.
* We handle the variations between PG and MYSQL types
* We use a combination of the source Database ``ID`` column and a last updated timestamp ``updated_at`` to show how Tinybird can quickly and easily reconstruct the table.

All of this is automatically created or removed for you using this script, barring the initial setup of the RDS instances of the source databases, and creating your Confluent Cluster and Tinybird Workspace. 

The demo script will:

1. Ensure the Source Database exists
2. Ensure the 'users' table is created
3. Generate a few initial events into the users table to be replicated
4. Create the appropriate Debezium Connector in Confluent Cloud
5. The Debezium connector will create the Kafka Topic, snapshot the initial table state, and start replicating changes
6. Ensure that Tinybird is has a Confluent connection to your Confluent Cloud cluster
7. Ingest the Topic into a 'raw' Tinybird Confluent Datasource containing all the user table change events.
8. Create a Tinybird Pipe which Materializes the users table with all changes in their final state
9. Publishes a simple Tinybird API which returns all non-deleted users.

You may re-run the demo script to generate further events to explore how they propagate through the pipeline.
You may also run the script with another switch to remove the Pipeline.

## Prereqs

You will need a Confluent Cloud account, an AWS Account, and a Tinybird Account, to work through this whole demo.
We'll set up AWS via the Console, the other actions are automated for you.
Finally, you will need various authentication keys for these services to allow the automation to work, the full set of keys is detailed in ``conf.py`` for your convenience.

### Python setup
You should use a virtualenv as good practice.
Once you have it set up, install the project requirements with 

```
pip install -r requirements.txt
```

 
## Getting Setup

Now we can walk through deploying the infrastructure.

### Get the repo

Clone this repo locally, and get ready to put the required information into ``conf.py``

### Confluent Cloud
If you don't already have a Confluent Cloud Environment and Kafka Cluster, you will need to create one.

### Tinybird
If you don't already have a Tinybird Workspace, you will need to create one.

### Using Postgres

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

### Using MySQL

Setting up MySQL follows much the same steps as Postgres above, with some variations detailed below.

When you create the parameter group in RDS, ensure that you do not select Aurora-Mysql, but plain MySQL (I'm using 5.7)

The parameters to set for MySQL are:
binlog_format=ROW
binlog_row_image=full

You must enable Backups as well.

Grab the connectivity information and put it into conf.py

## Using the Demo

In these following commands you should submit either 'PG' or 'MYSQL' as your Database type with the ``--source-db`` command. 

If you don't supply a ``--source-db`` it will default to Postgres.

2. Test your connectivity
```
python demo.py --test-connection --source-db PG
```

3. Create the Pipeline
Run the demo script to generate the full pipelines so you can explore it.
```
python demo.py --create-pipeline --source-db PG
```

5. Generate events
You can generate more events to see how the Pipeline works, or test the latency etc. by running the script without other switches.
```
python demo.py --source-db PG
```

4. Remove the Pipeline
You can remove the pipeline again using the removal command, this can also be used to recreate it by combining the commands
```
python demo.py --remove-pipeline --source-db PG
```

## Things to know

The Confluent connector for each Database type is automatically created for you, the process is quite simple and you can inspect it in the code. Note that the submission parameters are very speific and taken from the Confluent documentation; if you wish to experiment with other settings go right ahead.

Once created, the Connectors and Topics will appear in your Confluent Cloud console as usual.

The Topic is automatically created with a single partition for simplicity.

If you want to create with multiple partitions for performance, you will also probably want to ensure your rows are partitioned by ID to ensure updates easy to sequentially process.

The script attempts to use publicly documented APIs or processes, just executed within a simple python environment. We try to minimise dependencies.
One notable choice is using the Tinybird definition files instead of raw API calls, as this mimics typical user CLI behavior and they are easier to read than raw python objects.

## Troubleshooting

If you recreate the Table and Confluent setup, you also have to recreate the Tinybird raw Data Source otherwise it'll still be using the existing consumer group and offsets.

The script assumes your CONFLUENT_CLOUD_KEY has the same rights as you would in the console, and is not a limited access service account.

If you are experimenting with other settings you may wish to create the Connector in the Confluent UI first, as it has more robust checking. The API sometimes simply gives a 500 if it doesn't like a field you have submitted, whereas the UI is a little more verbose.

