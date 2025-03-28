import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1742908108577 = glueContext.create_dynamic_frame.from_catalog(database="customer-churn-s3-glue-database", table_name="customer_churn_data_landing_zone_bucket", transformation_ctx="AWSGlueDataCatalog_node1742908108577")

# Script generated for node Change Schema
ChangeSchema_node1742908173352 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1742908108577, mappings=[("customerid", "string", "CustomerID", "string"), ("city", "string", "City", "string"), ("zip code", "long", "Zip_Code", "int"), ("gender", "string", "Gender", "string"), ("senior citizen", "string", "Senior_Citizen", "string"), ("dependents", "string", "Dependents", "string"), ("tenure months", "long", "Tenure_Months", "int"), ("phone service", "string", "Phone_Service", "string"), ("multiple lines", "string", "Multiple_Lines", "string"), ("internet service", "string", "Internet_Service", "string"), ("online security", "string", "Online_Security", "string"), ("online backup", "string", "Online_Backup", "string"), ("device protection", "string", "Device_Protection", "string"), ("tech support", "string", "Tech_Support", "string"), ("streaming tv", "string", "Streaming_TV", "string"), ("streaming movies", "string", "Streaming_Movies", "string"), ("contract", "string", "Contract", "string"), ("paperless billing", "string", "Paperless_Billing", "string"), ("payment method", "string", "Payment_Method", "string"), ("monthly charges", "double", "monthly_charges", "float"), ("total charges", "double", "Total_Charges", "float"), ("churn label", "string", "Churn_Label", "string"), ("churn value", "long", "Churn_Value", "int"), ("churn score", "long", "Churn_Score", "int"), ("churn reason", "string", "Churn_Reason", "string")], transformation_ctx="ChangeSchema_node1742908173352")

# Script generated for node Amazon Redshift
AmazonRedshift_node1742909532839 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1742908173352, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-774305579900-af-south-1", "useConnectionProperties": "true", "dbtable": "public.customer_churn", "connectionName": "glue-connection-to-redshift", "preactions": "CREATE TABLE IF NOT EXISTS public.customer_churn (CustomerID VARCHAR, City VARCHAR, Zip_Code INTEGER, Gender VARCHAR, Senior_Citizen VARCHAR, Dependents VARCHAR, Tenure_Months INTEGER, Phone_Service VARCHAR, Multiple_Lines VARCHAR, Internet_Service VARCHAR, Online_Security VARCHAR, Online_Backup VARCHAR, Device_Protection VARCHAR, Tech_Support VARCHAR, Streaming_TV VARCHAR, Streaming_Movies VARCHAR, Contract VARCHAR, Paperless_Billing VARCHAR, Payment_Method VARCHAR, monthly_charges REAL, Total_Charges REAL, Churn_Label VARCHAR, Churn_Value INTEGER, Churn_Score INTEGER, Churn_Reason VARCHAR);"}, transformation_ctx="AmazonRedshift_node1742909532839")

job.commit()