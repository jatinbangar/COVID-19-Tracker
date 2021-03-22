from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    appName("COVID-19-Tracker"). \
    getOrCreate()

phi_df = spark. \
    read. \
    format("csv"). \
    option("inferSchema","true"). \
    option("header", "true"). \
    load("/user/hadoop/data/CA__covid19__latest.csv")

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
    schema("jh_Date date, jh_Country string, jh_Province string, jh_Lat double, jh_Long double, jh_Confirmed integer, \
            jh_Recovered integer, jh_Deaths integer"). \
    option("header", "true"). \
    load("/user/hadoop/data/time-series-19-covid-combined.csv")

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
         save("s3://covid-19-tracker-2020/output/tracker_results/")