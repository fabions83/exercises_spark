import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split, col, desc}

/*
Data is available in HDFS file system under /public/crime/csv
Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,
Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,
X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)
File format - text file
Delimiter - “,” (use regex while splitting split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1),
as there are some fields with comma and enclosed using double quotes.
Get top 3 crime types based on number of incidents in RESIDENCE area using “Location Description”
Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution03/RESIDENCE_AREA_CRIMINAL_TYPE_DATA
Output Fields: Crime Type, Number of Incidents
Output File Format: JSON
Output Delimiter: N/A
Output Compression: No

In Scala using Data Frames and SQL
 */

object Exercise3_DF {
  def main(array: Array[String]): Unit ={

      val spark = SparkSession.builder().master("yarn-client").appName("Exercise3_DF").getOrCreate()
      val crimes = spark.read.option("header", true).csv("/public/crime/csv/crimes.csv")

      val topCrimesInResidence = crimes
                .select(
                  split(col("Primary Type"),",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").getItem(0).alias("Crime Type"),
                  split(col("Location Description"),",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").getItem(0).alias("Location Description")
                )
                .filter(col("Location Description") === "RESIDENCE")
                .groupBy("Crime Type")
                .count()
                .withColumnRenamed("count", "Number of Incidents")
                .orderBy(desc("Number of Incidents"))
                .limit(3)

    topCrimesInResidence
      .coalesce(1)
      .write
      .json("/user/sandbox/solutions_sql/solution03/RESIDENCE_AREA_CRIMINAL_TYPE_DATA")
  }
}
