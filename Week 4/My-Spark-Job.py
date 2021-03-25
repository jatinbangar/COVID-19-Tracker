import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import date_format,to_date

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

from awsglue.dynamicframe import DynamicFrame

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "input_database", table_name = "phi_ca_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "input_database", table_name = "phi_ca_csv", transformation_ctx = "datasource0")
## @type: DataSource
## @args: [database = "input_database", table_name = "john_hopkins_csv", transformation_ctx = "datasource1"]
## @return: datasource1
## @inputs: []
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "input_database", table_name = "john_hopkins_csv", transformation_ctx = "datasource1")
phi_df=datasource0.toDF()
jh_df=datasource1.toDF()

phi_df1 = phi_df.select(phi_df.date.alias('phi_Date'), \
                        phi_df.prname.alias('phi_Province'), \
                        phi_df.numconf.alias('phi_Confirmed'), \
                        phi_df.numdeaths.alias('phi_Deaths') \
                       ). \
                 orderBy('phi_Province', 'phi_Confirmed', 'phi_Date')
                 
phi_df2 = phi_df1.withColumn("phi_Date", date_format(to_date(phi_df1.phi_Date, "dd-MM-yyyy"), "yyyy-MM-dd"))

jh_df1 = jh_df.withColumn('jh_Date', jh_df.date.cast('date')) \
    .withColumn('jh_Country', jh_df['country/region']) \
    .withColumn('jh_Province', jh_df['province/state']) \
    .withColumn('jh_Lat', jh_df.lat) \
    .withColumn('jh_Long', jh_df['long']) \
    .withColumn('jh_Confirmed', jh_df.confirmed) \
    .withColumn('jh_Recovered', jh_df.recovered) \
    .withColumn('jh_Deaths', jh_df.deaths) \
    .drop('date','country/region', 'province/state', 'lat', 'long', 'confirmed', 'recovered', 'deaths')

jh_Canada = jh_df1.filter(jh_df1.jh_Country == 'Canada')

phi_df3 = phi_df2.where("phi_Province NOT IN ('Nunavut', 'Repatriated travellers', 'Canada')")

jh_df2 = jh_Canada.where("jh_Province NOT IN ('Grand Princess', 'Diamond Princess')")

dfLeftOuterJoin = phi_df3. \
    join(jh_df2, \
         (phi_df3.phi_Date == jh_df2.jh_Date) & (phi_df3.phi_Province == jh_df2.jh_Province), \
         'left' \
        )

df_Final = dfLeftOuterJoin.select(dfLeftOuterJoin.phi_Date.alias('Date'), \
                                  dfLeftOuterJoin.jh_Country.alias('Country'), \
                                  dfLeftOuterJoin.phi_Province.alias('Province'), \
                                  dfLeftOuterJoin.jh_Lat.alias('Latitude'), \
                                  dfLeftOuterJoin.jh_Long.alias('Longitude'), \
                                  dfLeftOuterJoin.phi_Confirmed.alias('Confirmed'), \
                                  dfLeftOuterJoin.jh_Recovered.alias('Recovered'), \
                                  dfLeftOuterJoin.jh_Deaths.alias('Deaths') \
                                 ) \
                            .orderBy('Date','Country','Province')

## @type: ApplyMapping
## @args: [mapping = [("prname", "string", "phi_Province", "string"), ("date", "string", "phi_Date", "string"), ("numconf", "long", "phi_Confirmed", "long"), ("numdeaths", "long", "phi_Deaths", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
#applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("prname", "string", "phi_Province", "string"), ("date", "string", "phi_Date", "string"), ("numconf", "long", "phi_Confirmed", "long"), ("numdeaths", "long", "phi_Deaths", "long")], transformation_ctx = "applymapping1")

df_Final_Dynamic = DynamicFrame.fromDF(df_Final, glueContext, 'df_Final_Dynamic')

#df1 = ResolveChoice.apply(df_Final_Dynamic, choice = "make_cols")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://covid-19-tracker-2020/glue/output_data"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = df_Final_Dynamic, connection_type = "s3", connection_options = {"path": "s3://covid-19-tracker-2020/glue/output_data"}, format = "csv", transformation_ctx = "datasink2")
print("Job executed")
job.commit()