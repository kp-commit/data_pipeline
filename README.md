# Data Pipeline on Airflow
#### using AWS, Redshift, S3, JSON, Boto3, Python, ETL, SQL, DAG and Custom built operators
---

### Contents
        - Business Requirement
        - Solution
        - Datasets
        - File Contents
        - How to Run
        - Sample Execution
        - Common errors and resolution

---
### Business Requirement:
- A music streaming company, Sparkify, needs to build high grade data pipelines for their existing data warehouse.
- These need to transfer their current ETL pipelines into a tool that can **execute automatically**. When needed, should also allows for **mointoring** and **easy backfills**.
- Quality of data is a important aspect for data consumers i.e. analytics team. Thus, pipelines needs to **have built-in data checks** to catch any discrepanscies in the datasets.
- Source data resides in **S3** and needs to be processed in company's data warehouse in **Amazon Redshift**.
- The dataset consists of a directory with  **JSON logs** that describes user activity and **JSON metadata** about the song the users listen to in the application.



## Solution: Data Pipeline in Airflow using AWS Redshift
---
+  ##### Provisioned Redshift cluster on AWS using Boto3
    ######  **Setup AWS**
    1. Setup AWS and update creditinals in config
        - Manually setup an AWS account, obtained key and secret 
        - Set these in configuration file ```dwh.cfg```  
    ######  **Create Redshift Cluster**
    2.  Built Python script ```create_cluster.py``` to connected to **AWS** and create a **Redshift Cluster**:
        - Used ```Boto3``` to created ```iam```, ```redshift``` clients  
        - Launched a redshift cluster and created an IAM role that has read access to S3.
        - Added redshift database and IAM role info to ```dwh.cfg```  
        - Created ```iam role``` for cluster, obtained ```RoleArn```
        - Created a new cluster, obtained ```endpoint``` url
        - Set RoleArn and endpoint in ```dwh.cfg```
    ######  **Create Table Schemas**
    3. Created Table Schemas: Design schemas for your fact and dimension tables.
        - Initiated by ```create_tables.py``` executed ```create_drop_table_queries``` for ```CREATE```, ```DROP```
    ######  **Cluster util scripts** (run upon request)
    4. Built ```cluster_connect.py``` and ```cluster_status.py``` to allow Ad-hoc quering and check on cluster status.
    5. Built ```cluster_delete.py``` that deletes the Redshift cluster.

&nbsp;

+  ##### SETUP Data Pipeline in Airflow ![Workflow Diag](/images/dag.png)
    ######  **Setup Airflow**
    1. Setup Airflow with variables and connections
        - Created a new variable `redshift_iam_role` with AWS Role ARN under Airflow > Admin > Variables
        - Created a new connection `redshift` with info for Redshift under Airflow > Admin > Connections
    ###### **Create Custom Operator Classes**
    1. `class StageToRedshiftOperator` for **STAGE_EVENTS, STAGE_SONGS** tables
        - Created custom operator class in `stage_redshift.py` with COPY SQL template statement for Redshift. Used for parsing with dynamic params of:
            -  Redshift connection info
            -  Source tables
            -  Source S3 bucket + Prefix
            -  Source JSON metadata (if applicable)
            -  Target table
            -  Region
        - COPY with compression off and truncating columns for blanks values
    2. `class LoadDimensionOperator` for **USERS, SONGS, ARTISTS, TIME** tables
        - Created custom operator class in `load_dimension.py` with sql templates for TRUNCATE and INSERT statements. Used for parsing with dynamic params of:
            -  Redshift connection info
            -  Target tables
            -  Sub_query utilizing helper `class SqlQueries` in `sql_queries.py`
            -  Truncate  (Default TRUE)
    2. `class LoadFactOperator` for **SONGPLAYS** table
        - Created custom operator class in `load_fact.py` with sql templates for TRUNCATE and INSERT statements. Used for parsing with dynamic params of:
            -  Redshift connection info
            -  Target tables
            -  Sub_query utilizing helper `class SqlQueries` in `sql_queries.py`
            -  Truncate  (Default TRUE)
    1. `class DataQualityOperator` for **all** tables
        - Created custom operator in `data_quality.py` with data quality checks for records count checks.
    ###### **Create Directed Acyclic Graph (DAG)**
    1. DAG created in `dag.py` with params for :
        - start_date
        - end_date
        - depends_on_past: False
        - retries
        - retry_delay
        - catchup
    2. Tasks setup with `StageToRedshiftOperator`, `LoadDimensionOperator`, `LoadFactOperator` operators called with params given.
    3. Provided task order dependencies:
        ```python
        start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

        [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table 
        
        load_songplays_table >> [load_user_dimension_table, 
                                 load_song_dimension_table, 
                                 load_artist_dimension_table, 
                                 load_time_dimension_table]
        
        [load_user_dimension_table, 
         load_song_dimension_table, 
         load_artist_dimension_table, 
         load_time_dimension_table] >> run_quality_checks
        
        run_quality_checks >> end_operator
        ```



### Datasets:
---

Working with two datasets that reside in S3. Here are the S3 links for each: 

1. Song data: `s3://udacity-dend/song_data`
2. Log data: `s3://udacity-dend/log_data`,  
  JSON metadata: ```s3://udacity-dend/log_json_path.json``` 

**Song Dataset**

The first dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset. 

```
song_data/A/B/C/TRABCEI128F424C983.json 
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like:
```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
Log
```


**Log Dataset**

The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) with activity logs from a music streaming app based on specified configurations. The log files in the dataset are partitioned by year and month.

For example, here are filepaths to two files in this dataset. 
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```


And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

```
{"artist":"Blue October \/ Imogen Heap","auth":"Logged In","firstName":"Kaylee","gender":"F","itemInSession":7,"lastName":"Summers","length":241.3971,"level":"free","location":"Phoenix-Mesa-Scottsdale, AZ","method":"PUT","page":"NextSong","registration":1540344794796.0,"sessionId":139,"song":"Congratulations","status":200,"ts":1541107493796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"","userId":"8"}
```

![2018-11-12-events.json](/images/json.png)



### File Contents:
---
- **README.md** - README  file that includes summary, how to run and an explanation of all the files
- **images** - folder containing all images for README.md

##### redshift Folder:
- **dwh.cfg** - Configuration file with sections for AWS, DWH (Redshift Cluster), S3. Lists their parameters with values.
- **loadconfigs.py** - Python script to load and write all configuration params
- **env.sh** - Bash script to update environment PATH and make all .py files executeable
- **cluster_create.py** - Python script to create a Redshift Cluster
- **cluster_status.py** - Python script to check status of Redshift Cluster and get endpoint
- **cluster_connect.py** - Python script to check status of Redshift Cluster connection and run Ad-hoc queries
- **create_drop_table_queries.py** - contains all create and drop SQL queries used by `create_tables.py`
- **create_tables.py** - Python script to DROP any previously created tables and CREATE Redshift tables (Staging and Final Dimensions and Fact Tables) referencing queries in `create_drop_table_queries.py`
- **cluster_delete.py** - Python script to delete current Redshift Cluster


#####  airflow Folder:
- **dag folder** - contains `dag.py`
- **plugins folder** - contains operators, helpers and `__init__.py`
- **create_tables.sql** - template of CREATE SQLs used in redshift `create_drop_table_queries.py`
#####  plugins subFolder:
 `__init__.py` - initialization file
- operator subFolder:
    - `__init__.py` - initialization file
    - **stage_redshift.py** - custom operator with class StageToRedshiftOperator for STAGE_EVENTS, STAGE_SONGS tables
    - **load_dimension.py** - custom operator with class LoadDimensionOperator for USERS, SONGS, ARTISTS, TIME tables
    - **load_fact.py** - custom operator with class LoadFactOperator for SONGPLAYS table
    - **data_quality.py** - custom operator with class DataQualityOperator for data quality checks on all tables
- helpers subFolder:
    - `__init__.py` - initialization file
    - **sql_queries.py** - contains helper class SqlQueries with all sub query SQL statements
#####  dag subFolder:
- **dag.py** - Python script containing DAG that defines the data pipline with schedule, tasks and their dependencies with params like table, connection info etc by referencing all of the files listed prior.


### How to run:
---
#### 1. Setup AWS account:
- Open an AWS account and create an ACCESS KEY ID and a SECRET KEY to be used for EMR execution.
- Update these under **redshift Folder** in **dwh.cfg** for KEY and SECRET
- For more info, check AWS docs: [Create and Activate AWS Account](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/)  

#### 2. Create Redshift Cluster:
- **Open a terminal** and change to redshift scripts location: 
  `cd /home/workspace/redshift`
- **Modify permissions** to make **env.sh** executeable:
    `chmod +x env.sh` and execute `./env.sh`
- **Execute cluster_create.py**: `./cluster_create.py`

- Optionally, **Execute cluster_status.py**: `./cluster_status.py` to check cluster status.

#### 3. Create Table Schemas:
- **Execute create_tables.py**: `./create_tables.py`

#### 4. Setup Airflow:
Startup Airflow by executing `/opt/airflow/start.sh` and click on 'Access Airflow'
_**Note**_: Airflow will warn dag.py with variables not found. Ignore it, as this is our next step.

Within Airflow UI:
- Create a new variable `redshift_iam_role` with AWS Role ARN under Airflow > Admin > Variables
_**Note**_: you need to copy these from **redshift folder** file `dwh.cfg`, value for DWH_ROLE_ARN under section [DWH]

- Create a new connection `redshift` with info for Redshift under Airflow > Admin > Connections. Keep connection type as 'Postgres'.
_**Note**_: you need to copy these from **redshift folder** file `dwh.cfg`, values mentioned under section [DWH]. 

#### 4. Turn on DAG:
Within Airflow UI:
- Turn on DAG, toggle to ON
![Crawler created](/images/turn_on_dag.png)

#### 5. Monitor Execution and check Logs
Within Airflow UI:
- Click on `dag` (the name of the DAG). This will bring up Graph or Tree View with all task current status.
- Click on a task and select 'View log' to see logs.

#### 6. Optionally, connect and run Ad-hoc queries
- From terminal run `./cluster_connect.py`
_**Note**_: You can modify queries with the file to execute specific queries. Sample queries provided as reference.

#### 7. Optionally, delete cluster
- From terminal run `./cluster_delete.py`



### Sample Run:
---

##### 1. Updated **dwh.cfg** under redshift folder
```bash
[AWS]
key = mykey
secret = mysecert

[DWH]
dwh_cluster_type = multi-node
dwh_num_nodes = 4
dwh_node_type = dc2.large
dwh_iam_role_name = dwhRole
dwh_cluster_identifier = dwhCluster
dwh_db = dwh
dwh_db_user = dwhuser
dwh_db_password = Passw0rd
dwh_port = 5439
dwh_role_arn = 
dwh_endpoint = 

[S3]
log_data = s3://udacity-dend/log_data
log_jsonpath = s3://udacity-dend/log_json_path.json
song_data = s3://udacity-dend/song_data
```

##### 2. Gave permission to execute and ran **env.sh**
```bash
root@f8b518cd3ee7:/home/workspace/# cd redshift
root@f8b518cd3ee7:/home/workspace/redshift# chmod +x env.sh
root@f8b518cd3ee7:/home/workspace/redshift# ./env.sh
PATH updated
/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/workspace
*.py files made executeable
root@f8b518cd3ee7:/home/workspace/redshift# 
```

#### 3. Created Table Schemas:
```
root@f8b518cd3ee7:/home/workspace/redshift# ./create_tables.py 
Droping Tables in Cluster...Dropped!
Creating Tables in Cluster...Created!
```

#### 4. Setup Airflow:
```
root@f8b518cd3ee7:/home/workspace/redshift# /opt/airflow/start.sh 
```
![Airflow startup warning](/images/Normal_startup__variable_connection_not_setup_yet.png)

Within Airflow UI:
- Create a new variable `redshift_iam_role` with AWS Role ARN under Airflow > Admin > Variables
![Airflow variable setup](/images/redshift_iam_role_variable_setup.png)

- Create a new connection `redshift` with info for Redshift under Airflow > Admin > Connections. Kept connection type as 'Postgres'.
![Airflow variable setup](/images/redshift_connection_setup.png)

#### 4. Turned on DAG:
Within Airflow UI:
- Turn on DAG, toggle to ON
![Crawler created](/images/turn_on_dag.png)

#### 5. Monitor Execution and check Logs
Within Airflow UI:
- Click on `dag` (the name of the DAG).
![Good execution run](/images/execution_good.png)

#### 6. Optionally, delete cluster
- From terminal run `./cluster_delete.py`
```bash
root@f8b518cd3ee7:/home/workspace# ./cluster_delete.py 
Detaching IAM Role
Deleting Cluster
                 Key                                                                                  Value
0  ClusterIdentifier  dwhcluster                                                                           
1  NodeType           dc2.large                                                                            
2  ClusterStatus      deleting                                                                             
3  MasterUsername     dwhuser                                                                              
4  DBName             dwh                                                                                  
5  Endpoint           {'Address': 'dwhcluster.clustername.us-west-2.redshift.amazonaws.com', 'Port': 5439}
6  VpcId              vpc-555d200c                                                                         
7  NumberOfNodes      2                                                                                    

Deleting Cluster...###########################An error occurred (ClusterNotFound) when calling the DescribeClusters operation: Cluster dwhcluster not found.
DELETED. Cluster not present now.
```

### Common errors and resolution
##### 1. Warning on startup with Airflow:
![Variable issue](/images/Normal_startup__variable_connection_not_setup_yet.png)
**Resolution:** Can be safe ignored, variables need to be setup under Airflow Admin
![Connection setup](/images/redshift_connection_setup.png)
![Variable setup](/images/redshift_iam_role_variable_setup.png)

##### 2. Startup issue (dag error):
![Startup issue](/images/issue2.png)
**Resolution:** Syntax issue with code addressed.

##### 3. Execution issue:
![Execution issue](/images/issue1.png)
**Checked logs**
![Task log](/images/issue1_log.png)
**Resolution:** Table params need to be fixed, task cleared and dag toogled back on. 
![Task log](/images/issue1_resolution_bug_in_params.png)
![Clear task](/images/issue1_clear_log_after_resolution.png)
![Dag on](/images/turn_on_dag.png)

##### 4. IAM_role_error
![IAM_role_error](/images/IAM_role_error.png)
**Resolution:** Variable not being read within dag, variable call updated.
