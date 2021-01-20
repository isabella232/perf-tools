# CDF Pipeline automation python script

This python script creates, deploy and run CDF data pipeline from exported json.

Python script creates 8 different compute profiles named 5wn_profile,10wn_profile,15wn_profile,20wn_profile,30wn_profile,60wn_profile,2wn_profile,4wn_profile.
Compute profiles are created from compute_profile_1.json,compute_profile_2.json,compute_profile_3.json,compute_profile_4.json,compute_profile_5.json,compute_profile_6.json,compute_profile_7.json,compute_profile_8.json these json files.
Runtime arguments/macros of cdf pipelines are specified in Runtime_argument_json directory.Change the values of macros accordingly.
### Data Generation: 
- Data Generates using Datagenerator plugin for all pipelines except multitable->multibq and pub/sub->trash/bq pipelines.
- Data generates from DataGenerator->BQ/GCS pipelines are used as source input where GCS and BQ is source.

        Note:For pipelines where GCS is source,use data generated from Datagen -> GCS pipeline with 10k splits
        for 2TB and 3TB data sizes.

### Pub/sub-> BQ/Trash pipeline:
- In pubsub cdf pipelines data is published into pubsub topics using java code.
- Pubsub topic and subscriptions are created using terraform script.Use these topics and subscription in runtime arguments/macros json file.

        Note: Start java script to publish message in topic when CDF pipeline is in running state.

## Prerequisites

- Download Mssql-server jdbc driver for database plugin from [here](https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver15).Install this driver in cdf instance before running multitable to multibq pipeline.

- Install Multi-table and Trash plugin from HUB in CDF instance.

- Download and Install DataGenerator plugin in CDF instance from [here](https://github.com/data-integrations/data-generator).
### Serivce Account
Create a service account with the following permission
- Cloud Data Fusion Admin 



**To run Python script use following command :** 

    $python3 main.py <region> <cdf_instance_name> <compute_profile> <pipeline_name> <pipeline_json_path> <runtime_arg_json_path>

* region = Name of CDF instance region
* cdf_instance_name = Name of CDF instance
* compute_profile = Name of compute profile which mentioned above.
* pipeline_name = Name of the CDF pipeline.
* pipeline_json_path = Path of exported json file.
* runtime_arg_json_path = path of Runtime argument/macros json file. 

Example: $python3 main.py us-west1  uc4-script-test 10wn_profile Datagen_to_GCS_1TB ./pipelines/Datagen_to_GCS_1TB-cdap-data-pipeline.json ./Runtime_argument_json/Datagen_to_gcs.json


