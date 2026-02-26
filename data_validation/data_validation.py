import awswrangler as wr
import pandas as pd
import logging
import boto3
import json
import re
import watchtower
from datetime import datetime
import configs_file
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pyspark
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
from pyspark.sql.functions import current_timestamp
from zoneinfo import ZoneInfo

# Initialize Spark
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
spark.conf.set('spark.sql.catalog.AwsDataCatalog', 'org.apache.iceberg.spark.SparkCatalog')
spark.conf.set('spark.sql.catalog.AwsDataCatalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
spark.conf.set('spark.sql.catalog.AwsDataCatalog.warehouse', configs_file.config_params['ICEBERG_WAREHOUSE'])
spark.conf.set('spark.sql.catalog.AwsDataCatalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
spark.conf.set("spark.sql.session.timeZone", "Asia/Bahrain")
spark.conf.unset("spark.sql.catalog.AwsDataCatalog.write.object-storage.path")
# Configuration parameters
ETL_DB = configs_file.config_params['ETL_DB']
TARGET_DB_IN_ATHENA = configs_file.config_params['TARGET_DB_IN_ATHENA']
OUTPUT_LOCATION = configs_file.config_params['OUTPUT_LOCATION']
TARGET_DB_CATALOG = configs_file.config_params['TARGET_DB_CATALOG']
ETL_MASTER_DB = configs_file.config_params['ETL_MASTER_DB']
DATA_VALIDATION_TABLE = configs_file.config_params['DATA_VALIDATION_TABLE']
GROUP_ID = configs_file.config_params['GROUPID']
SOURCE_SECRET_KEY = configs_file.config_params['SOURCE_SECRET_KEY']
JOB_NAME = configs_file.config_params['JOB_NAME']
BAHRAIN_TZ = ZoneInfo("Asia/Bahrain")

# =========================
# SNS CONFIG
# =========================
SNS_TOPIC_ARN = configs_file.config_params['SNS_TOPIC_ARN']

def format_duration(start_time, end_time):
    total_seconds = int((end_time - start_time).total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def format_execution_time(start_time, end_time):
    total_seconds = int((end_time - start_time).total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return f"{hours:02} Hour {minutes:02} Minutes {seconds:02} Seconds"


def generate_batch_number(groupid: int) -> str:
    bahrain_now = datetime.now(BAHRAIN_TZ)
    epoch_seconds = int(bahrain_now.timestamp())
    return f"{epoch_seconds}_{groupid}"


def send_sns_notification(subject: str, message: dict):
    """
    Send SNS notification with JSON message
    """
    try:
        sns_client = boto3.client("sns", region_name="me-south-1")
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],  # SNS subject max 100 chars
            Message=json.dumps(message, default=str)
        )
    except Exception as e:
        logging.exception(f"Failed to send SNS notification: {e}")


def send_job_start_sns(job_name, groupid, start_time, batch_number):
    try:
        subject = f"[Batch:{batch_number}] Data Validation Job: {job_name} Started"

        message = {
            "job_name": job_name,
            "batch_number": batch_number,
            "groupid": groupid,
            "start_time": start_time
        }

        send_sns_notification(subject, message)

    except Exception as e:
        logging.exception(
            f"Error while sending JOB_STARTED SNS | "
            f"job_name={job_name}, groupid={groupid}, batch_number={batch_number} | {e}"
        )


def send_job_completion_sns(job_name, groupid, start_time,
                            end_time, batch_number):
    try:
        subject = f"[Batch:{batch_number}] Data Validation Job: {job_name} Completed"
        execution_time = format_execution_time(start_time, end_time)
        message = {
            "job_name": job_name,
            "batch_number": batch_number,            
            "groupid": groupid,
            "start_time": start_time,
            "end_time": end_time,
            "execution_time": execution_time
        }

        send_sns_notification(subject, message)

    except Exception as e:
        logging.exception(
            f"Error while sending JOB_COMPLETED SNS | "
            f"job_name={job_name}, groupid={groupid}, batch_number={batch_number} | {e}"
        )


def send_job_error_sns(job_name, groupid,
                       start_time, batch_number, error_message):
    try:
        subject = f"[ERROR] [Batch:{batch_number}] {job_name}"

        message = {
            "message": "Data Validation Job Failed",
            "job_name": job_name,
            "batch_number": batch_number,            
            "groupid": groupid,
            "start_time": start_time,
            "error_message": error_message[:2000]
        }

        send_sns_notification(subject, message)

    except Exception as e:
        logging.exception(
            f"Error while sending JOB_FAILED SNS | "
            f"job_name={job_name}, groupid={groupid}, batch_number={batch_number} | {e}"
        )


def get_cloudwatch_logger(log_group_name):
    client = boto3.client('logs')
    logger = logging.getLogger('my_logger')
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(filename)s | %(funcName)s | %(message)s')
    handler = watchtower.CloudWatchLogHandler(boto3_client=client, log_group=log_group_name)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def get_secret_name(SECRET_KEY):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name='me-south-1')
    try:
        get_secret_value_response = client.get_secret_value(SecretId=SECRET_KEY)
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret
    except Exception as e:
        logging.exception(f"Failed to retrieve secrets: {e}")
        return {}


def read_from_source(spark, jdbc_url, query, username, password, driver):
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", driver) \
            .option("fetchsize", "1000000") \
            .load()
        df_schema = df.printSchema()
        logging.info("df_schema: ")
        logging.info(df_schema)
        count_value = df.collect()[0]['CNT']
        return count_value
    except Exception as e:
        logging.exception(f"Error reading query from source: {query}\n{e}")
        raise


def read_sql_query(sql_query, database, data_source):
    try:
        return wr.athena.read_sql_query(
            sql=sql_query,
            database=database,
            data_source=data_source,
            ctas_approach=False,
            s3_output=OUTPUT_LOCATION
        )
    except Exception as e:
        logging.exception(f"Error reading query from Athena: {sql_query}\n{e}")
        raise


def read_from_oracle_source(spark, jdbc_url, query, username, password, driver):
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", driver) \
            .option("fetchsize", "100000") \
            .load()
        df_schema = df.printSchema()
        logging.info("df_schema  ")
        logging.info(df_schema)
        count_value = df.collect()[0]['CNT']
        return count_value
    except Exception as e:
        logging.exception(f"Error reading query from Source: {query}\n{e}")
        raise


def write_iceberg_table(spark, diff_df, TARGET_DB_CATALOG, ETL_MASTER_DB, DATA_VALIDATION_TABLE, logger):
    try:
        schema = StructType([
            StructField("ID", LongType(), True),
            StructField("JOB_NAME", StringType(), True),
            StructField("GROUPID", LongType(), True),
            StructField("TABLE_NAME", StringType(), True),
            StructField("LOAD_TYPE", StringType(), True),
            StructField("INCREMENTAL_COLUMN", StringType(), True),
            StructField("SOURCE_COUNT", LongType(), True),
            StructField("DATALAKE_COUNT", LongType(), True),
            StructField("COUNT_DIFF", LongType(), True),
            StructField("STARTDATETIME", TimestampType(), True),
            StructField("START_OFFSET", LongType(), True),
            StructField("STATUS", StringType(), True),
            StructField("ERROR_MESSAGE", StringType(), True),
            StructField("SRC_QUERY_EXEC_TIME", StringType(), True),
            StructField("TARGET_QUERY_EXEC_TIME", StringType(), True),
            StructField("BATCH_NUMBER", StringType(), True)
        ])
        spark_df = spark.createDataFrame(diff_df, schema=schema)
        spark_df.createOrReplaceTempView("spark_df")

        spark_df_with_curr_ts = spark.sql("""
            SELECT ID, JOB_NAME, GROUPID, TABLE_NAME, LOAD_TYPE, INCREMENTAL_COLUMN,
                   SOURCE_COUNT, DATALAKE_COUNT, COUNT_DIFF, STARTDATETIME, START_OFFSET,
                   current_timestamp() AS INSERT_TIMESTAMP, STATUS, ERROR_MESSAGE, SRC_QUERY_EXEC_TIME, TARGET_QUERY_EXEC_TIME, BATCH_NUMBER
            FROM spark_df
        """)

        spark_df_with_curr_ts.createOrReplaceTempView("spark_df_with_curr_ts")

        query_str = f"INSERT INTO {TARGET_DB_CATALOG}.{ETL_MASTER_DB}.{DATA_VALIDATION_TABLE} SELECT * FROM spark_df_with_curr_ts"
        logger.info(f"Executing query to write to Iceberg table: {query_str}")
        return spark.sql(query_str)

    except Exception as e:
        logging.exception(f"Error writing to Iceberg table {DATA_VALIDATION_TABLE}: {e}")
        raise


def build_athena_validation_expr(data_validation_column: str) -> str:
    cols = [c.strip() for c in data_validation_column.split(",")]
    if len(cols) == 1:
        return cols[0]
    else:
        return "coalesce(" + ", ".join(cols) + ")"


def transform_offset_query(sql_query_existing: str) -> str:
    return re.sub(r'>\s*\{offset_value\}', '<= {offset_value}', sql_query_existing)


def transform_sql_query(sql_query: str, start_dt_str: str, data_validation_column: str) -> str:
    pattern_parent = r"WHERE\s+PARENT\.\w+\s*>\s*TO_TIMESTAMP\(\{startdatetime\}.*?\)"
    replacement_parent = f"""WHERE PARENT.{data_validation_column} > TO_TIMESTAMP('{start_dt_str}', 'YYYY-MM-DD HH24:MI:SS.FF') - INTERVAL '30' DAY AND PARENT.{data_validation_column} <= TO_TIMESTAMP('{start_dt_str}', 'YYYY-MM-DD HH24:MI:SS.FF')"""
    if re.search(pattern_parent, sql_query, flags=re.IGNORECASE):
        return re.sub(pattern_parent, replacement_parent, sql_query, flags=re.IGNORECASE)

    pattern_generic = r"WHERE\s+(t\.)?(\w+)\s*>\s*TO_TIMESTAMP\(\s*('.*?'|\{startdatetime\})\s*,\s*'YYYY-MM-DD HH24:MI:SS\.FF'\s*\)"

    def repl(match):
        alias = match.group(1) or ""
        return (
            f"WHERE {alias}{data_validation_column} > TO_TIMESTAMP('{start_dt_str}', 'YYYY-MM-DD HH24:MI:SS.FF') - INTERVAL '30' DAY "
            f"AND {alias}{data_validation_column} <= TO_TIMESTAMP('{start_dt_str}', 'YYYY-MM-DD HH24:MI:SS.FF')"
        )

    if re.search(pattern_generic, sql_query, flags=re.IGNORECASE):
        return re.sub(pattern_generic, repl, sql_query, flags=re.IGNORECASE)

    return sql_query


def transform_non_oracle_sql_query(sql_query: str, start_dt_str: str, data_validation_column: str) -> str:
    cols = [c.strip() for c in data_validation_column.split(",")]

    if len(cols) == 1:
        expr = f"TRY_CONVERT(datetime2(3), t.{cols[0]})"
    else:
        expr = "COALESCE(" + ", ".join([f"TRY_CONVERT(datetime2(3), t.{c})" for c in cols]) + ")"

    replacement = (
        f"WHERE {expr} > DATEADD(DAY, -30, CAST('{start_dt_str}' AS datetime2(3))) "
        f"AND {expr} <= CAST('{start_dt_str}' AS datetime2(3))"
    )

    return re.sub(
        r"WHERE\s+.*",
        replacement,
        sql_query,
        flags=re.IGNORECASE | re.DOTALL
    )


def process_table_row(row, logger, start_time, batch_number):
    try:
        id = row['id']
        groupid = row['groupid']
        tbl = row['tablename']
        loadtype = row['loadtype']
        start_dt = row['startdatetime']
        data_validation_column = row['data_validation_column']
        offset_value = row['offset_value']
        schemaname = row['schemaname']
        source_type = row['source']
        sql_query_existing = row['sql_query']

        retrieved_secret_values = get_secret_name(SOURCE_SECRET_KEY)
        jdbc_url = str(retrieved_secret_values['url'])
        username = str(retrieved_secret_values['user'])
        password = str(retrieved_secret_values['password'])
        driver = str(retrieved_secret_values['driver'])

        # start_dt_str = pd.to_datetime(start_dt).strftime('%Y-%m-%d %H:%M:%S')
        start_dt_str = pd.to_datetime(start_dt).strftime('%Y-%m-%d %H:%M:%S.%f')

        # Choose the correct reader function
        source_reader = read_from_oracle_source if source_type == 'oracle' else read_from_source
        src_start = datetime.now()
        if loadtype == 'Full Load':
            src_query = f"(SELECT COUNT(*) AS CNT FROM ({sql_query_existing}) subq) tmp"
            target_query = f"SELECT count(*) FROM {tbl}"
            logger.info(f"Executing existing source query: {sql_query_existing}")
            logger.info(f"Executing source query: {src_query}")
            src_df_count = source_reader(spark, jdbc_url, src_query, username, password, driver)
            src_end = datetime.now()
            src_exec_time = format_duration(src_start, src_end)
            logger.info(f"Executing target query: {target_query}")
            tgt_start = datetime.now()
            target_df_count = read_sql_query(target_query, TARGET_DB_IN_ATHENA, TARGET_DB_CATALOG)
            tgt_end = datetime.now()
            tgt_exec_time = format_duration(tgt_start, tgt_end)
        elif loadtype.lower() in ('merge', 'incremental'):
            if pd.notna(offset_value):
                if source_type == 'oracle':
                    # sql_query_with_offset = sql_query_existing.format(offset_value=offset_value)
                    sql_query_transform = transform_offset_query(sql_query_existing)
                    sql_query_with_offset = f"{sql_query_transform}".replace("{offset_value}", str(offset_value))
                    src_query = f"(SELECT /*+ PARALLEL(5) */  COUNT(*) AS CNT FROM ({sql_query_with_offset}) subq) tmp"
                else:
                    # sql_query_with_offset = sql_query_existing.format(offset_value=offset_value)
                    sql_query_transform = transform_offset_query(sql_query_existing)
                    sql_query_with_offset = f"{sql_query_transform}".replace("{offset_value}", str(offset_value))
                    src_query = f"(SELECT COUNT(*) AS CNT FROM ({sql_query_with_offset}) subq) tmp"
            else:
                if source_type == 'oracle':
                    sql_query_modified = transform_sql_query(sql_query_existing, start_dt_str, data_validation_column)
                    src_query = f"(SELECT /*+ PARALLEL(5) */  COUNT(*) AS CNT FROM ({sql_query_modified}) subq) tmp"
                else:
                    sql_query_modified = transform_non_oracle_sql_query(sql_query_existing, start_dt_str,
                                                                        data_validation_column)
                    src_query = f"(SELECT COUNT(*) AS CNT FROM ({sql_query_modified}) subq) tmp"
            logger.info(f"Executing existing source query: {sql_query_existing}")
            logger.info(f"Executing source query: {src_query}")
            src_start = datetime.now()
            src_df_count = source_reader(spark, jdbc_url, src_query, username, password, driver)
            src_end = datetime.now()
            src_exec_time = format_duration(src_start, src_end)
            if pd.notna(offset_value):
                target_query = f"SELECT count(*) FROM {tbl}"
            else:
                athena_expr = build_athena_validation_expr(data_validation_column)
                target_query = f"SELECT count(*) FROM {tbl} WHERE {athena_expr} > TIMESTAMP '{start_dt_str}' - INTERVAL '30' DAY"

            logger.info(f"Executing target query: {target_query}")
            tgt_start = datetime.now()
            target_df_count = read_sql_query(target_query, TARGET_DB_IN_ATHENA, TARGET_DB_CATALOG)
            tgt_end = datetime.now()
            tgt_exec_time = format_duration(tgt_start, tgt_end)
        else:
            logger.warning(f"Unknown load type: {loadtype} for table {tbl}")
            return

        src_count = src_df_count
        tgt_count = int(target_df_count.iloc[0, 0])
        diff_count = src_count - tgt_count

        diff_df = pd.DataFrame([{
            "ID": id,
            "JOB_NAME": JOB_NAME,
            "GROUPID": groupid,
            "TABLE_NAME": tbl,
            "LOAD_TYPE": loadtype,
            "INCREMENTAL_COLUMN": data_validation_column,
            "SOURCE_COUNT": int(src_count),
            "DATALAKE_COUNT": int(tgt_count),
            "COUNT_DIFF": int(diff_count),
            "STARTDATETIME": start_dt,
            "START_OFFSET": offset_value,
            "STATUS": "SUCCESS",
            "ERROR_MESSAGE": None,
            "SRC_QUERY_EXEC_TIME": src_exec_time,
            "TARGET_QUERY_EXEC_TIME": tgt_exec_time,
            "BATCH_NUMBER": batch_number
        }])

        diff_df = diff_df.where(pd.notnull(diff_df), None)
        logger.info(f"Source count for table {tbl}: {src_count}, Datalake count for table {tbl}: {tgt_count}")
        logger.info(
            f"successfully writing to data validation table: ID={id}, JOB_NAME='{JOB_NAME}', GROUPID ={groupid}, TABLE_NAME='{tbl}', LOAD_TYPE='{loadtype}',INCREMENTAL_COLUMN ='{data_validation_column}', SOURCE_COUNT ={src_count}, DATALAKE_COUNT={tgt_count}, COUNT_DIFF={diff_count},STARTDATETIME=Timestamp'{start_dt}',START_OFFSET ={offset_value}, INSERT_TIMESTAMP=Timestamp'{start_time}', STATUS='SUCCESS', ERROR_MESSAGE=None, src_time={src_exec_time}, tgt_time={tgt_exec_time}")

        write_iceberg_table(spark, diff_df, TARGET_DB_CATALOG, ETL_MASTER_DB, DATA_VALIDATION_TABLE, logger)
        return diff_df, None

    except Exception as e:
        logger.exception(f"Error processing table row: {row.to_dict()} \n{e}")
        error_message = str(e)[:1000]
        error_df = pd.DataFrame([{
            "ID": id,
            "JOB_NAME": JOB_NAME,
            "GROUPID": groupid,
            "TABLE_NAME": tbl,
            "LOAD_TYPE": loadtype,
            "INCREMENTAL_COLUMN": data_validation_column,
            "SOURCE_COUNT": None,
            "DATALAKE_COUNT": None,
            "COUNT_DIFF": None,
            "STARTDATETIME": start_dt,
            "START_OFFSET": offset_value,
            "STATUS": "FAILED",
            "ERROR_MESSAGE": error_message,
            "SRC_QUERY_EXEC_TIME": None,
            "TARGET_QUERY_EXEC_TIME": None,
            "BATCH_NUMBER": batch_number
        }])

        error_df = error_df.where(pd.notnull(error_df), None)
        logger.info(
            f"failed ,writing to data validation table: ID={id}, JOB_NAME='{JOB_NAME}', GROUPID ={groupid}, TABLE_NAME='{tbl}', LOAD_TYPE='{loadtype}',INCREMENTAL_COLUMN ='{data_validation_column}', SOURCE_COUNT =None, DATALAKE_COUNT=None, COUNT_DIFF=None,STARTDATETIME=Timestamp'{start_dt}',START_OFFSET ={offset_value}, INSERT_TIMESTAMP= Timestamp'{start_time}', STATUS='FAILED', ERROR_MESSAGE='{error_message}'")

        write_iceberg_table(spark, error_df, TARGET_DB_CATALOG, ETL_MASTER_DB, DATA_VALIDATION_TABLE, logger)
        return None, error_df


def main():
    logger = get_cloudwatch_logger(f'asb-logs-dev-bah-lakehouse-efts-validation-logs-01-hq18k51')
    all_success_dfs = []
    all_error_dfs = []
    try:
        job_start_time = datetime.now(BAHRAIN_TZ)
        logger.info(f"Job started at: {job_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        batch_number = generate_batch_number(GROUP_ID)
        logger.info(f"Generated batch_number for job: {batch_number}")

        etl_master_sql_query = f"""
            SELECT id, groupid, sql_query, source, schemaname, tablename,
                   loadtype,CAST(startdatetime AS timestamp(6)) AS startdatetime, data_validation_column, offset_value
            FROM etl_master
            WHERE groupid = {GROUP_ID} and active = 1 and data_validation_required = 1 order by priority asc;
        """
        logger.info(f"etl_master_sql_query : {etl_master_sql_query}")
        data_df = read_sql_query(etl_master_sql_query, ETL_DB, TARGET_DB_CATALOG)
        logger.info("Sending JOB_STARTED SNS notification...")
        send_job_start_sns(
            job_name=JOB_NAME,
            groupid=GROUP_ID,
            start_time=job_start_time,
            batch_number=batch_number
        )
        logger.info("JOB_STARTED SNS notification sent successfully.")

        for _, row in data_df.iterrows():
            try:
                success_df, error_df = process_table_row(row, logger, job_start_time, batch_number)
                if success_df is not None:
                    all_success_dfs.append(success_df)
                if error_df is not None:
                    all_error_dfs.append(error_df)
            except Exception as inner_e:
                logger.error(f"Table {row.get('tablename')} failed to process: {inner_e}")
                continue

        if all_success_dfs or all_error_dfs:
            results_df = pd.concat(all_success_dfs + all_error_dfs)
        else:
            results_df = pd.DataFrame()

        job_end_time = datetime.now(BAHRAIN_TZ)
        #success_tables = [df.iloc[0]['TABLE_NAME'] for df in all_success_dfs]
        #failed_tables = [df.iloc[0]['TABLE_NAME'] for df in all_error_dfs]
        #failed_tables = [{"tablename": df.iloc[0]['TABLE_NAME'], "error": df.iloc[0]['ERROR_MESSAGE']} for df in all_error_dfs]               
        logger.info("Sending JOB_COMPLETED SNS notification...")
        send_job_completion_sns(
            job_name=JOB_NAME,
            groupid=GROUP_ID,
            start_time=job_start_time,
            end_time=job_end_time,
            batch_number=batch_number
        )

        logger.info("JOB_COMPLETED SNS notification sent successfully.")
        logger.info("Job completed successfully.")
    except Exception as e:
        logger.exception(f"Unhandled exception in main(): {e}")
        try:
            send_job_error_sns(
                job_name=JOB_NAME,
                groupid=GROUP_ID,
                start_time=job_start_time,
                batch_number=batch_number if 'batch_number' in locals() else None,
                error_message=str(e)
            )          
            logger.info("JOB_FAILED SNS notification sent successfully.")
            
        except Exception as email_e:
            logger.error(f"Failed to send failure email: {email_e}")


if __name__ == "__main__":
    main()
