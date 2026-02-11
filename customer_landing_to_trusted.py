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

# Script generated for node customerLanding
customerLanding_node1770764886846 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-han/landing/customer/"], "recurse": True}, transformation_ctx="customerLanding_node1770764886846")

# Script generated for node SQL Query
SqlQuery942 = '''
select * from myDataSource
where 
shareWithResearchAsOfDate is not null;
'''
SQLQuery_node1770767954102 = sparkSqlQuery(glueContext, query = SqlQuery942, mapping = {"myDataSource":customerLanding_node1770764886846}, transformation_ctx = "SQLQuery_node1770767954102")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1770767954102, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1770767530809", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1770768202795 = glueContext.getSink(path="s3://stedi-lake-house-han/trusted/customer/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1770768202795")
customer_trusted_node1770768202795.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customer_trusted")
customer_trusted_node1770768202795.setFormat("json")
customer_trusted_node1770768202795.writeFrame(SQLQuery_node1770767954102)
job.commit()