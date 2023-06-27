
# Hotel Data Transformation Pyspark

Requirement of this project is to get the Hotel data from link and create Apache Spark application in Python.


## Installation
we need to install putty, a tool used for generating and managing SSH key pairs that are required for secure communication with EMR clusters.
    
## AWS services used in this project
EMR cluster(spark,select latest version of spark in emr),S3 storage ,putty.
## Cluster
we have used YARN to run spark application as per the project requirement.
YARN: YARN is a cluster resource manager that can be used to run Spark applications. YARN manages the resources on the cluster, including the CPU, memory, and storage. This means that Spark does not need to worry about managing these resources itself. YARN is a good choice for large clusters or for production deployments.

To submit a Spark application in "yarn-cluster" mode we have used 
this query,

usr/bin/spark-submit --master yarn --deploy-mode client --driver-memory 3g --executor-memory
2g --num-executors 1 --executor-cores 1 /home/hadoop/hotel.py
## Running the Job
we can direct run our job by creating shell scripting,we will create shell script in that we perform required transformation and also we get data by defining S3 bucket path in scripting.And after performing transformations we save the required table sets in particuar output folder in S3 bucket in parquet format.