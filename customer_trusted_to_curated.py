import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1770769204331 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-han/trusted/accelerometer/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1770769204331")

# Script generated for node customer_trusted
customer_trusted_node1770769149344 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-han/trusted/customer/"], "recurse": True}, transformation_ctx="customer_trusted_node1770769149344")

# Script generated for node Customer with Accelerometer Readings Filter
CustomerwithAccelerometerReadingsFilter_node1770769236224 = Join.apply(frame1=accelerometer_trusted_node1770769204331, frame2=customer_trusted_node1770769149344, keys1=["user"], keys2=["email"], transformation_ctx="CustomerwithAccelerometerReadingsFilter_node1770769236224")

# Script generated for node Drop Accelerometer Fields
DropAccelerometerFields_node1770769281562 = DropFields.apply(frame=CustomerwithAccelerometerReadingsFilter_node1770769236224, paths=["z", "timestamp", "x", "y", "user"], transformation_ctx="DropAccelerometerFields_node1770769281562")

# Script generated for node Drop Duplicates
DropDuplicates_node1770769314534 =  DynamicFrame.fromDF(DropAccelerometerFields_node1770769281562.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1770769314534")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1770769314534, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1770769059218", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1770769321498 = glueContext.getSink(path="s3://stedi-lake-house-han/curated/customer/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="customer_curated_node1770769321498")
customer_curated_node1770769321498.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customer_curated")
customer_curated_node1770769321498.setFormat("json")
customer_curated_node1770769321498.writeFrame(DropDuplicates_node1770769314534)
job.commit()