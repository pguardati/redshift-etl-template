# redshift-etl-template

Template of an ETL pipeline to load data into a Redshift database.  
The template is based on a Data Warehouse project,  
designed for the Data Engineering Nanodegree of Udacity.


## Design
This Data Warehouse is designed to:
- store information from user choices and song records until a given point in time.
- expose these information to the analytics team.

The raw data are stored in a **S3 bucket** while  
the data warehouse is hosted in **Amazon Redshift**.

The **staging** part consists in 2 tables:
- staging_songs, that stores the song records
- staging_events, that stores the log records

The **analytics** part has been implemented using a star schema with 5 tables:
- 1 Fact table (songplays)
- 4 Dimension tables (songs, artists, users, time)

Note: Query execution is **distributed**.  
The staging tables have been created in order to achieve balanced distribution
over nodes when the query to join these table is triggered.

The analytics tables are designed to:
- be partitioned by row with a round robin policy, in case the table is big
- be allocated in each node during a query, in case the table is small

## Installation

Before to start:  
Add the current project folder path to PYTHONPATH.  
In ~/.bashrc, append: 
```
PYTHONPATH=your/path/to/repo:$PYTHONPATH 
export PYTHONPATH
```
e.g.
```
PYTHONPATH=~/PycharmProjects/SparkifyRedshiftDB:$PYTHONPATH 
export PYTHONPATH
```

To install and activate the environment:
```
conda env create -f environment.yml
conda activate sparkify_redshift_db 
```

To use this software an aws account is needed.  
Also, a configuration file named *dwh_launch.cfg* 
has to be created under *SparkifyRedshiftDB/credentials*.  
it has to contain the aws credentials and the infrastructure configuration,
with the following format:
```
[AWS]
KEY=YoUr_AwS_Key
SECRET=YoUr_sEcReT_aWs_pAsSwOrD

[HARDWARE]
CLUSTER_TYPE=multi-node
CLUSTER_NUM_NODES=4
CLUSTER_NODE_TYPE=dc2.large
CLUSTER_IDENTIFIER=dwhCluster

[DATABASE]
DB_NAME=dwh
DB_USER=dwhuser
DB_PASSWORD=Passw0rd
DB_PORT=5439

[SECURITY]
DWH_IAM_ROLE_NAME=dwhRole
DWH_SECURITY_GROUP_ID=sg-292b4502
```

## Usage
To create the infrastructure:
```
python sparkify_redshift_db/scripts/create_infrastructure.py
```

To drop the current tables and create new empty ones:
```
python sparkify_redshift_db/scripts/create_tables.py
```

To run the etl pipeline on the full data from s3
```
python sparkify_redshift_db/scripts/etl.py
```

To check the content of the database, run:
```
python sparkify_redshift_db/scripts/check_database.py
```

## Tests
To run all unittests:
```
python -m unittest discover sparkify_redshift_db/tests
```

## IMPORTANT NOTE
when the task is finished,  
remember to delete the aws infrastructure with:
```
python sparkify_redshift_db/scripts/delete_infrastructure.py
```
