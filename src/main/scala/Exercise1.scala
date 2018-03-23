import org.apache.spark.{SparkContext, SparkConf}

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

In Scala using Core API
*/


object Exercise1 {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Exercise1").setMaster("yarn-client")
    val sc = new SparkContext(conf)

    val crimes_csv = sc.textFile("/public/crime/csv/crimes.csv")
    val header = crimes_csv.first()
    val crimes = crimes_csv.filter(crime => crime != header)

    val crimesDatePrimarytype = crimes.map(crime => {
      val crime_split = crime.split(",")
      val date = crime_split(2).split(" ")(0).split("/")
      val date_format = date(2) + date(0)
      val primaryType = crime_split(5)
      ((date_format.toInt, primaryType), 1)
    }).cache()

    val crimesReduce = crimesDatePrimarytype
      .reduceByKey((total, crime) => total + crime)
      .map(crime => ((crime._1._1, -crime._2), (crime._1._1 + "\t" + crime._2 + "\t" + crime._1._2)))
      .sortByKey()

    crimesReduce.saveAsTextFile("/user/sandbox/solutions/solution01/crimes_by_type_by_month",
      classOf[org.apache.hadoop.io.compress.GzipCodec])

  }
}