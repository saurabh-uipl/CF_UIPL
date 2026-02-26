from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import sys
from botocore.errorfactory import ClientError
import datetime
from DataIngestionFramework.src.data_migration import DataMigration
from DataIngestionFramework.src.etl_config import ETLConfig
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .appName("DataIngestionFramework")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.uipl",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.uipl.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.uipl.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.uipl.warehouse",
            "s3://asb-s3-dev-bah-lakehouse-raw-01-urq47pi3/s3_files/")
    .config("spark.sql.session.timeZone", "Asia/Bahrain")
    .getOrCreate()
)
sc = spark.sparkContext
hadoopConf = sc._jsc.hadoopConfiguration()
#spark.conf.set("spark.sql.session.timeZone", "IST")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.session.timeZone", "Asia/Bahrain")
#spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
### HADOOP PARAMETERS #####

#hadoopConf.set("fs.s3.maxRetries", "1000")
hadoopConf.set("fs.s3.maxRetries", "60")
hadoopConf.set("fs.s3a.experimental.input.fadvise", "random")
hadoopConf.set("fs.s3a.experimental.fadvise", "random")
hadoopConf.set("fs.s3a.readahead.range", "2048K")
hadoopConf.set("fs.s3a.fast.upload", "true")

### SPARK TUNING PARAMETERS #####
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.sql.shuffle.partitions", 500)
spark.conf.set("spark.sql.files.maxPartitionBytes", 31457280)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 524288000)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 209715200)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 629145600)
spark.conf.set("spark.sql.broadcastTimeout", 900)
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
spark.conf.set("spark.hadoop.fs.s3a.experimental.input.fadvise", "random")
spark.conf.set("spark.hadoop.fs.s3a.readahead.range", "2048K")
spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
spark.conf.set("spark.hadoop.parquet.filter.stats.enabled", "true")
spark.conf.set("spark.hadoop.fs.s3a.experimental.fadvise", "random")
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
spark.conf.set("spark.sql.decimalOperations.allowPrecisionLoss","true")
spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
# spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
 
   ###	ETLConfig(source_secret_name=secret name to access source db, glueContext,
   ###          redshift_secret_name= secret name to access redshift, region_name="region_name", job_name='JOBNAME',
   ###          sns_topic='NA', smtp_ses_secret_name= secret name for ses,
   ###          inventory_database='NA', data_chunk_size='NA', iceberg_db='NA',
   ###          db_name=redshift db in which etl_master is stored, job_id= groupid, 
   ###		 redshift_schema_name= redshift schema in which etl_master is stored,
   ###          redshift_audit_table_name= name of audit table present in redshift, 
   ###		 redshift_etl_master_table_name=name of etl master table present in redshift, athena_db_name= name of database created in athena,
   ###          log_group='NA', iceberg_etl_master_table_name='NA', iceberg_audit_table_name='NA', timezone_param= Asia/Kolkata)
  
etl_config = ETLConfig(
    source_secret_name='asb-sm-dev-bah-lakehouse-efts-mssql-01-qihs6101',
    glueContext=spark,
    redshift_secret_name='NA',
    region_name='me-south-1',
    job_name='S3_TO_ICEBERG',
    sns_topic='arn:aws:sns:me-south-1:136399769509:asb-sns-dev-bah-lakehouse-data-sync-files-process-alerts-01-m1u4d5',
    smtp_ses_secret_name='NA',
    inventory_database='NA',
    data_chunk_size='NA',
    iceberg_db='asb_athena_dev_bah_lakehouse_framework_01',
    db_name='NA',
    job_id=2,
    redshift_schema_name='NA',
    redshift_audit_table_name='NA',
    redshift_etl_master_table_name='NA',
    athena_db_name='t24_post_cob_db',
    log_group='asb-logs-dev-bah-lakehouse-s3-file-process-01-yq27fj61',
    iceberg_etl_master_table_name='etl_master',
    iceberg_audit_table_name='audit_table',
    timezone_param='Asia/Bahrain',
    redshift_dest_db='NA',
    redshift_dest_schema='NA',
    rs_iam_role_arn='NA',
    # Athena query result bucket (must be full S3 path)
    rs_temp_bucket_uri='s3://asb-s3-dev-bah-lakehouse-temp-01-51ioql94/athena-query-results/'
)
                     
data_migration_obj = DataMigration(etl_config)
is_success = data_migration_obj.start_ingestion_job()
is_success = is_success[0]
 
if int(is_success) > 0:
    print("job run successfully ")
