import sys
from pyspark.sql import SparkSession

phi_path = sys.argv[1]
john_hopkins_path = sys.argv[2]
output_path = sys.argv[3]

spark = SparkSession. \
    builder. \
    appName("Covid Tracker"). \
    getOrCreate()

phi_df = spark. \
    read. \
    format("csv"). \
    option("inferSchema","true"). \
    option("header", "true"). \
    load(phi_path)

phi_df1 = phi_df.select(phi_df.date.alias('phi_Date'), \
                        phi_df.prname.alias('phi_Province'), \
                        phi_df.numconf.alias('phi_Confirmed'), \
                        phi_df.numdeaths.alias('phi_Deaths') \
                       ). \
                 orderBy('phi_Province', 'phi_Confirmed', 'phi_Date')

from pyspark.sql.functions import date_format, to_date

phi_df2 = phi_df1.withColumn("phi_Date", date_format(to_date(phi_df1.phi_Date, "dd-MM-yyyy"), "yyyy-MM-dd"))

phi_df3 = phi_df2.where("phi_Province NOT IN ('Repatriated travellers', 'Canada')")

jh_df = spark. \
    read. \
    format("csv"). \
    schema("jh_Date date, jh_Country string, jh_Province string, jh_Lat double, jh_Long double, \
            jh_Confirmed integer, jh_Recovered integer, jh_Deaths integer"). \
    option("header", "true"). \
    load(john_hopkins_path)

jh_df1 = jh_df.filter(jh_df.jh_Country == 'Canada')

jh_df2 = jh_df1.where("jh_Province NOT IN ('Repatriated Travellers', 'Grand Princess', 'Diamond Princess')")

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
                          .orderBy("Province", "Date", "Confirmed")

df_Final.write. \
         format("csv"). \
         mode("overwrite"). \
         save("output_path")