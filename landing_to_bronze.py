import sys
import hashlib
import boto3
import json
from datetime import datetime,timezone
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (current_timestamp,input_file_name,lit)

#Audit Lambda integration

lambda_client = boto3.client("lambda",region_name = "ca-central-1")
def send_audit_record(audit_row): 
    payload ={"row":audit_row}
    response = lambda_client.invoke(FunctionName = "bo-dl-dev-runlog-writer",InvocationType ="RequestResponse",Payload=json.dumps(payload).encode("utf-8"))
    result = json.loads(response["Payload"].read().decode("utf-8"))
    if response.get("FunctionError"):
        raise RuntimeError(f"Run log writer lambda failed:{result}")
    if result.get("status")!="SUCCESS":
        raise RuntimeError(f"Audit write was not confirmed:{result}")
    print("Audit record inserted:",result)
    return result 
    
# Read job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME","environment","report_name","source_system","source_format","target_format","target_database","source_path","target_path"])

# Initialize Glue and spark

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
print("glue runtime argument names:")
for arg in sys.argv:
    print(arg.split("=")[0] if "=" in arg else arg)
glue_client = boto3.client("glue",region_name = "ca-central-1")
job_name = args["JOB_NAME"]
glue_run_id = None 
for i,arg in enumerate(sys.argv):
    if arg == "--JOB_RUN_ID" and i +1 <len(sys.argv):
        glue_run_id = sys.argv[i+1]
        break
if not glue_run_id or not glue_run_id.startswith("jr_"):
    raise RuntimeError("Valid Glue JOB_RUN_ID not found")
print(f"Glue run ID:{glue_run_id}")
            
logger = glueContext.get_logger()
audit_started_utc = datetime.now(timezone.utc)

# Read Configuration
source_path = args ["source_path"]
target_path = args ["target_path"]
source_format = args["source_format"]
target_format = args ["target_format"]

logger.info("landing to bronze job started")

# Read source data

reader = spark.read.format(source_format)
if source_format.lower()=="json":reader = reader.option("multiLine","true").option("mode","FAILFAST")
df = reader.load(source_path)

# Validate record count

input_count = df.count()
if input_count == 0:
    raise ValueError("source data is empty")
logger.info(f"Input record count:{input_count}")

# null value validation - test rule

from pyspark.sql.functions import col 
mandatory_field = "acdlogon_agtoid"
if mandatory_field not in df.columns:
    raise ValueError (f"Required field is missing:{mandatory_field}")

from pyspark.sql.functions import col,lit

#Identify invalid records
invalid_df = df.filter(col(mandatory_field).isNull())
null_count = invalid_df.count()

if null_count > 0:

    # add rejection reason
    quarantine_df = invalid_df.withColumn("rejection_reason",lit(f"Null value in {mandatory_field}"))

    #write invalid records to quarantine
    input_id = hashlib.sha256(source_path.encode("utf-8")).hexdigest()[:16]
    quarantine_path = ("s3://blue-lake-landing-quarantine-dev/"
    "iex/agent-activity/negative_test/"f"input_id={input_id}/")
    quarantine_df.write.mode("overwrite").parquet(quarantine_path)
    logger.info(f"Quarantined{null_count} invalid records")
    
logger.info("Null value validation completed")

#Continue processing only valid records
valid_df = df.filter(col(mandatory_field).isNotNull())
valid_count = valid_df.count()
logger.info(f"valid records available for bronze:{valid_count}")


# Add audit metadata

bronze_df = (valid_df.withColumn("_ingestion_timestamp",current_timestamp())
                .withColumn("_source_file",input_file_name())
                .withColumn("_source_system",lit(args["source_system"]))
                .withColumn("_report_name",lit(args["report_name"])))
                
# write data into bronze
if valid_count > 0:
    bronze_df.write.format(target_format).mode("errorifexists").save(target_path)
    logger.info("Bronze data writes completed")
else:    
    logger.info("No valid records.skipping bronze write")
logger.info(f"Processed valid records:{valid_count}")

# commit glue job
audit_completed_utc = datetime.now(timezone.utc)

# Build audit record

audit_row = {
    "run_id": glue_run_id,
    "parent_run_id": None ,
    "report": "shared",
    "pipeline": "iex-hourly",
    "step": "l2b",
    "service": "GLUE",
    "service_run_id": glue_run_id,
    "execution_arn": f"glue-job-run-{glue_run_id}",
    "attempt": 1,
    "source": "iex",
    "dataset": "agent-activity",
    "target_table": "bronze_workforce.test_run_02",
    "run_mode": "MANUAL",
    "status": "SUCCESS",
    "started_utc": audit_started_utc.isoformat().replace("+00:00","Z"),
    "ended_utc": audit_completed_utc.isoformat().replace("+00:00","Z"),
    "rows_in": input_count,
    "rows_out": valid_count
}

#send audit record to lambda
send_audit_record(audit_row)


job.commit()
logger.info("Landing to Bronze job completed")

