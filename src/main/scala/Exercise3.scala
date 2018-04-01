import org.apache.spark.{SparkContext, SparkConf}

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

In Scala using Core API
 */

object Exercise3 {
  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("yarn-client").setAppName("Exercise3")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val crimesWithHeader = sc.textFile("/public/crime/csv/crimes.csv")
    val header = crimesWithHeader.first
    val crimes = crimesWithHeader.filter(crime => crime != header)

    val crimesMap = crimes
      .map(crime => {
          val crimeSplit = crime.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
          (crimeSplit(5), crimeSplit(7))
        })
      .filter(crime => crime._2 == "RESIDENCE")
      .cache()

    val crimesInResidence = crimesMap
      .map(crime => (crime._1, 1))
      .reduceByKey((total, crime) => total + crime)
      .cache()

    val topCrimesInResidence = sc.parallelize(crimesInResidence
        .map(crime => ((-crime._2), (crime._1, crime._2)))
        .sortByKey()
        .take(3))

    topCrimesInResidence
          .map(crime => crime._2)
          .toDF("Crime Type", "Number of Incidents")
          .coalesce(1)
          .write
          .json("/user/sandbox/solutions/solution03/RESIDENCE_AREA_CRIMINAL_TYPE_DATA")
  }
}
