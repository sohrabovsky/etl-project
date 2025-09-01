Introduction:
In this pipeline project, first we create data for mongodb, then we load data to postgres, our data warehouse.
The project contains:
- warehouse schema: warehouse_schema.sql
- required python packages: requrements.txt
- script for loading data in mongodb: seed_data.py
- script for aggregating data in mongodb and sending aggreagated data to postgres: etl_pipeline.py
- a docker-compose.yaml file for running mongodb and postgres containers
- a folder for data as import data in mongodb: data
- an export folder for exported csv and json reports after etl running: exports

Running the pipeline:
For running the pipline there is a cronjob format which can easily implemented by writing command:
crontab -e and then type with your favorit text editor such as vim:
* 10 * * * python3 /path/to/folder/etl_pipeline.py
