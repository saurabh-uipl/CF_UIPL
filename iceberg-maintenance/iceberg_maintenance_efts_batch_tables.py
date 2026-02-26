import sys
import pytz
import boto3
import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import watchtower
import logging
import configs_file
from pyspark.conf import SparkConf

conf = (
    pyspark.SparkConf()
    # SQL Extensions
    # can not set property dynamically ---
    # .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    # Configuring Catalog
    .set('spark.sql.catalog.AwsDataCatalog', 'org.apache.iceberg.spark.SparkCatalog')
    .set('spark.sql.catalog.AwsDataCatalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
    .set('spark.sql.catalog.AwsDataCatalog.warehouse', configs_file.config_params['ICEBERG_WAREHOUSE'])
    .set('spark.sql.catalog.AwsDataCatalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set('spark.sql.iceberg.handle-timestamp-without-timezone', True)
    .set('spark.sql.iceberg.use-timestamp-without-timezone-in-new-tables', True)
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
CLOUDWATCH_LOG_GROUP = configs_file.config_params['CLOUDWATCH_LOG_GROUP']

def get_cloudwatch_logger(log_group):
    logger = logging.getLogger("data-compaction")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = watchtower.CloudWatchLogHandler(
            boto3_client=boto3.client("logs", region_name="me-south-1"),
            log_group=log_group
        )
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(filename)s | %(funcName)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


logger = get_cloudwatch_logger(CLOUDWATCH_LOG_GROUP)

athena_client = boto3.client('athena', region_name='me-south-1')

database_list = configs_file.config_params['DATABASE_LIST']

for db_name in database_list:
    logger.info("Processing Tables Of " + str(db_name))

    paginator = athena_client.get_paginator('list_table_metadata')

    response_iterator = paginator.paginate(CatalogName='AwsDataCatalog', DatabaseName=db_name)

    tablesToCompact = []
    for page in response_iterator:
        tablesToCompact.extend((i['Name'] for i in page['TableMetadataList']))
    logger.info(tablesToCompact)

    for table in tablesToCompact:
        try:
            if 'd1_' in table:
                logger.info("Excluding ..........."+str(table))
            else:
                logger.info("Starting to Compact: " + str(table))
    
                ### procedure rewrite_data_files : Rewrite the data files in table using the default rewrite algorithm of bin-packing to combine small files and also split large files according to the default write size of the table.
                spark.sql("CALL AwsDataCatalog.system.rewrite_data_files(table => '{0}.{1}')".format(db_name, table)).show()
    
                ### procedure rewrite_manifests : Rewrite the manifests in table and align manifest files with table partitioning.
                spark.sql("CALL AwsDataCatalog.system.rewrite_manifests(table => '{0}.{1}')".format(db_name, table)).show()
    
                ### procedure expire_snapshots : used to remove older snapshots and their files which are no longer needed.
                spark.sql("CALL AwsDataCatalog.system.expire_snapshots(table => '{0}.{1}')".format(db_name, table)).show()
    
                ### procedure remove_orphan_files : used to remove files which are not referenced in any metadata files of an Iceberg table .
                spark.sql("CALL AwsDataCatalog.system.remove_orphan_files(table => '{0}.{1}')".format(db_name, table)).show(200, False)
    
                logger.info("Ended Compaction : " + str(table))
        except Exception as e:
            logger.info(str(e))

logger.info("Compaction Job Completed")
