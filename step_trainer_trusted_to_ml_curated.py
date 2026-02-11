import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1770772825907 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-han/trusted/step-trainer/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1770772825907")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1770772826503 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-han/trusted/accelerometer/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1770772826503")

# Script generated for node SQL Query
SqlQuery826 = '''
select a.*,b.* from trainer as a
inner join accelerometer as b 
on a.sensorreadingtime = b.timestamp

'''
SQLQuery_node1770773802314 = sparkSqlQuery(glueContext, query = SqlQuery826, mapping = {"trainer":StepTrainerTrusted_node1770772825907, "accelerometer":AccelerometerTrusted_node1770772826503}, transformation_ctx = "SQLQuery_node1770773802314")

# Script generated for node ML Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1770773802314, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1770772787307", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MLCurated_node1770772980357 = glueContext.getSink(path="s3://stedi-lake-house-han/curated/machine_learning/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="MLCurated_node1770772980357")
MLCurated_node1770772980357.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="learning_curated")
MLCurated_node1770772980357.setFormat("json")
MLCurated_node1770772980357.writeFrame(SQLQuery_node1770773802314)
job.commit()