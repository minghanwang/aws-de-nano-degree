import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node accelerometer_landing
accelerometer_landing_node1770766090140 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-han/landing/accelerometer/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1770766090140")

# Script generated for node customer_trusted
customer_trusted_node1770766810202 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-han/trusted/customer/"], "recurse": True}, transformation_ctx="customer_trusted_node1770766810202")

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1770768565388 = Join.apply(frame1=customer_trusted_node1770766810202, frame2=accelerometer_landing_node1770766090140, keys1=["email"], keys2=["user"], transformation_ctx="CustomerPrivacyFilter_node1770768565388")

# Script generated for node SQL Query
SqlQuery806 = '''
select * from myDataSource
WHERE FROM_UNIXTIME(timeStamp / 1000e0) > FROM_UNIXTIME(shareWithResearchAsOfDate / 1000e0);
'''
SQLQuery_node1770768657821 = sparkSqlQuery(glueContext, query = SqlQuery806, mapping = {"myDataSource":CustomerPrivacyFilter_node1770768565388}, transformation_ctx = "SQLQuery_node1770768657821")

# Script generated for node Drop Fields
DropFields_node1770768726390 = DropFields.apply(frame=SQLQuery_node1770768657821, paths=["shareWithFriendsAsOfDate", "phone", "lastUpdateDate", "email", "customerName", "shareWithResearchAsOfDate", "birthDay", "shareWithPublicAsOfDate", "serialNumber", "registrationDate"], transformation_ctx="DropFields_node1770768726390")

# Script generated for node Drop Duplicates
DropDuplicates_node1770768873401 =  DynamicFrame.fromDF(DropFields_node1770768726390.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1770768873401")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1770768873401, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1770768282689", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1770768898534 = glueContext.getSink(path="s3://stedi-lake-house-han/trusted/accelerometer/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1770768898534")
accelerometer_trusted_node1770768898534.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1770768898534.setFormat("json")
accelerometer_trusted_node1770768898534.writeFrame(DropDuplicates_node1770768873401)
job.commit()