# Do all imports and installs here
import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType
from pyspark.sql.functions import udf, date_format, split, monotonically_increasing_id
import datetime as dt

date_adding_udf = udf(lambda z: date_adding(z))


def start_spark_session():
    """Create and return Spark Session
    Access keys required for reading/writing data to s3
    saurfang:spark-sas7bdat required to read sas format data
    """
    return SparkSession.builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.5") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .enableHiveSupport().getOrCreate()


def date_adding(days):
    """UDF to convert SAS numeric dates to standard dates"""
    if (days):
        date = dt.datetime(1960, 1, 1).date()
        return (date + dt.timedelta(days)).isoformat()
    return None


def clean_weather(df_temps):
    """Cleans weather DF by removing nulls, selecting needed columns and splitting the date into Year and month"""
    df_temps = df_temps.filter(df_temps.AverageTemperature.isNotNull())
    split_temp = split(df_temps['dt'], '-')
    df_temps = df_temps.withColumn('Year', split_temp.getItem(0))
    df_temps = df_temps.withColumn('Month', split_temp.getItem(1))
    df_temps = df_temps.select("City", "Country", "AverageTemperature", "Year", "Month", "Latitude", "Longitude")
    return df_temps


def clean_airports(df_airports):
    """Cleans airports df
    removes all airports without Iata Code and not within the US and selects needed columns
    """
    df_airports = df_airports.where("iso_country = 'US' and iata_code is not null")
    df_airports = df_airports.select("ident", "type", "name", "elevation_ft", "municipality", "gps_code", "iata_code",
                                     "local_code", "coordinates")
    return df_airports


def clean_demographics(df_cities):
    """Cleans Demographics DF
    Gives city an ID and cleans up column names
    """
    df_cities1 = df_cities.selectExpr("monotonically_increasing_id() as Id", "*")
    df_cities = df_cities1
    for col in df_cities1.columns:
        df_cities = df_cities.withColumnRenamed(col, col.replace(" ", "_"))
    return df_cities


def clean_immigration(df_immi, df_iata):
    """Cleans Immigration DF
    Drops unneeded Columns
    joins Iata code to get state and city details
    cleans up data format of dates and numbers
    """
    df_immi = df_immi.drop('occup', 'insnum', 'entdepu', 'i94yr', 'admnum', 'dtaddto', 'entdepa', 'matflag', 'entdepa',
                           'count', 'bdtadfile', 'dtadfile', 'i94cit', 'i94res')
    df_immi = df_immi.join(df_iata, df_immi.i94port == df_iata.Code)
    df_immi= df_immi.drop(df_immi.Code)
    df_immi = df_immi.withColumn("biryear", df_immi["biryear"].cast(IntegerType()))
    df_immi = df_immi.withColumn("i94mon", df_immi["i94mon"].cast(IntegerType()))
    df_immi = df_immi.withColumn("cicid", df_immi["cicid"].cast(IntegerType()))
    df_immi = df_immi.withColumn("arrdate", df_immi["arrdate"].cast(IntegerType()))
    df_immi = df_immi.withColumn("arrdate", date_adding_udf('arrdate'))
    df_immi = df_immi.withColumn("depdate", df_immi["depdate"].cast(IntegerType()))
    df_immi = df_immi.withColumn("depdate", date_adding_udf('depdate'))

    return df_immi


def quality_checks(spark, immi, demographics, temps, airports):
    """Performs quality checks on the dataframes before they are exported
    checks for primary key nulls and duplicates in the fact table
    checks rows exist for all tables
    returns false if any errors occur
    """
    immi.createOrReplaceTempView("immi_view")
    fact_nulls = spark.sql("""SELECT COUNT(*) as count FROM immi_view WHERE cicid IS NULL""")
    fact_duplicates = spark.sql(
        """select sum(*) as sum from (SELECT COUNT(cicid) FROM immi_view group by cicid HAVING COUNT(*) > 1) as A""")
    null_count = fact_nulls.first()['count']
    dupe_count = fact_duplicates.first()['sum']
    tcheck = temps.count()
    acheck = airports.count()
    dcheck = demographics.count()
    icheck = immi.count()

    if (dupe_count is None):
        dupe_count = 0
    print('Count of Primary Key nulls in Fact Table: ', null_count, ', Count of Primary Key Duplicates in Fact Table: ',
          dupe_count)
    print('Table Row Counts:', 'Temperatures ', tcheck, ', Airports ', acheck, ', Demographics ', dcheck,
          ', Immigration ', icheck)

    if (tcheck == 0 | acheck == 0 | dcheck == 0 | icheck == 0 | null_count > 0 | dupe_count > 0):
        return False
    return True

def output_files(output_data, airports, demographics, immi, temps):
    """Write files to S3"""
    airports.write.mode("overwrite").parquet(output_data + "Airports")
    temps.write.partitionBy("Month").mode("overwrite").parquet(output_data + "Temperatures")
    immi.write.mode("overwrite").parquet(output_data + "Immigration")
    demographics.write.mode("overwrite").parquet(output_data + "Demographics")




def main():
    """Loads config and runs ETL Job"""
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    os.environ['AWS_ACCESS_KEY_ID'] = config['default']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['default']['AWS_SECRET_ACCESS_KEY']
    output_data = "S3_BUCKET_HERE"
    input_data = "S3_BUCKET_HERE"

    spark = start_spark_session()

    df_cities = spark.read.csv(input_data +"us-cities-demographics.csv", sep=';', header=True)
    df_temps = spark.read.csv(input_data +"GlobalLandTemperaturesByCity.csv", sep=',', header=True)
    df_iata = spark.read.csv(input_data +"iata-codes.csv", sep='-',
                             header=True)  # reference source https://www.airportcodes.us/us-airports.htm
    df_airports = spark.read.csv(input_data +"airport-codes_csv.csv", sep=',', header=True)
    df_immi = spark.read.format('com.github.saurfang.sas.spark').load(input_data + 'i94_apr16_sub.sas7bdat')

    airports = clean_airports(df_airports)
    demographics = clean_demographics(df_cities)
    immi = clean_immigration(df_immi, df_iata)
    temps = clean_weather(df_temps)
    quality = quality_checks(spark, immi, demographics, temps, airports)
    
    #if quality checks are passed write to S3
    if(quality):
        #output_files(output_data, airports, demographics, immi, temps)
        print("quality checks passed, data written to s3")
    else:
        print("quality checks failed, no data written")


if __name__ == "__main__":
    main()
