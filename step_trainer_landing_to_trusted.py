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

# Script generated for node CustomerCurated
CustomerCurated_node1770769817558 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-han/curated/customer/"], "recurse": True}, transformation_ctx="CustomerCurated_node1770769817558")

# Script generated for node StepTrainerLanding
StepTrainerLanding_node1770769818546 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-han/landing/step-trainer/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1770769818546")

# Script generated for node innerjoin
SqlQuery817 = '''
select b.* from CustomerCurated as a 
inner join steptrainer as b
on a.serialNumber = b.serialNumber
'''
innerjoin_node1770772315796 = sparkSqlQuery(glueContext, query = SqlQuery817, mapping = {"CustomerCurated":CustomerCurated_node1770769817558, "steptrainer":StepTrainerLanding_node1770769818546}, transformation_ctx = "innerjoin_node1770772315796")

# Script generated for node Drop Duplicates
DropDuplicates_node1770772443685 =  DynamicFrame.fromDF(innerjoin_node1770772315796.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1770772443685")

# Script generated for node step-trainer-trusted
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1770772443685, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1770772044551", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
steptrainertrusted_node1770772526547 = glueContext.getSink(path="s3://stedi-lake-house-han/trusted/step-trainer/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1770772526547")
steptrainertrusted_node1770772526547.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="step_trainer_trusted")
steptrainertrusted_node1770772526547.setFormat("json")
steptrainertrusted_node1770772526547.writeFrame(DropDuplicates_node1770772443685)
job.commit()