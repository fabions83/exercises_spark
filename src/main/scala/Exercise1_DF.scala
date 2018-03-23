import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split, concat, desc, lit}

/*
Exercise 01 - Get monthly crime count by type

Data is available in HDFS file system under /public/crime/csv
Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,
Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,
Updated On,Latitude,Longitude,Location)
File format - text file
Delimiter - “,”
Get monthly count of primary crime type, sorted by month in ascending and number of crimes
per type in descending order
Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution01/crimes_by_type_by_month
Output File Format: TEXT
Output Columns: Month in YYYYMM format, crime count, crime type
Output Delimiter: \t (tab delimited)
Output Compression: gzip

In Scala using Data Frames and SQL
*/

object Exercise1_DF {
  def main(args: Array[String]){
    val spark = SparkSession.builder().appName("Exercise1_DF").master("yarn-client").getOrCreate()

    val crimes = spark.read.option("header", true).csv("/public/crime/csv/crimes.csv")
    val crimes_df = crimes.withColumn(
      "DateFormat", concat(split(split(col("Date"), "/")(2), " ")(0),
        split(col("Date"), "/")(0)).cast("int")
    ).select("DateFormat", "Primary Type").cache()

    val crimesGroup = crimes_df
      .groupBy("DateFormat", "Primary Type").count()
      .orderBy(col("DateFormat"), desc("count"))
      .select(concat(col("DateFormat"), lit("\t"), col("count"), lit("\t"), col("Primary Type")))

    crimesGroup
      .coalesce(1)
      .write
      .option("compression", "gzip")
      .text("/user/sandbox/solutions_sql/solution01/crimes_by_type_by_month")

  }
}