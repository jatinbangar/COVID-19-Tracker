import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "csvdatafroms3", table_name = "john_hopkins_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "csvdatafroms3", table_name = "john_hopkins_csv", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("date", "string", "jh_date", "string"), ("country/region", "string", "jh_country", "string"), ("province/state", "string", "jh_province", "string"), ("lat", "double", "jh_lat", "double"), ("long", "double", "jh_long", "double"), ("confirmed", "long", "jh_confirmed", "long"), ("recovered", "long", "jh_recovered", "long"), ("deaths", "long", "jh_deaths", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("date", "string", "jh_date", "string"), ("country/region", "string", "jh_country", "string"), ("province/state", "string", "jh_province", "string"), ("lat", "double", "jh_lat", "double"), ("long", "double", "jh_long", "double"), ("confirmed", "long", "jh_confirmed", "long"), ("recovered", "long", "jh_recovered", "long"), ("deaths", "long", "jh_deaths", "long")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["jh_province", "jh_deaths", "jh_confirmed", "jh_lat", "jh_long", "jh_date", "jh_country", "jh_recovered"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["jh_province", "jh_deaths", "jh_confirmed", "jh_lat", "jh_long", "jh_date", "jh_country", "jh_recovered"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "redshiftschemafortables", table_name = "dev_public_jh_table", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "redshiftschemafortables", table_name = "dev_public_jh_table", transformation_ctx = "resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")
## @type: DataSink
## @args: [database = "redshiftschemafortables", table_name = "dev_public_jh_table", redshift_tmp_dir = TempDir, transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "redshiftschemafortables", table_name = "dev_public_jh_table", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
job.commit()