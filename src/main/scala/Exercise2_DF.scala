import org.apache.spark.sql.SparkSession

/*
Exercise 02 - Get details of inactive customers

Data is available in local file system /data/retail_db
Source directories: /data/retail_db/orders and /data/retail_db/customers
Source delimiter: comma (“,”)
Source Columns - orders - order_id, order_date, order_customer_id, order_status
Source Columns - customers - customer_id, customer_fname, customer_lname and many more
Get the customers who have not placed any orders, sorted by customer_lname
and then customer_fname
Target Columns: customer_lname, customer_fname
Number of files - 1
Target Directory: /user/<YOUR_USER_ID>/solutions/solutions02/inactive_customers
Target File Format: TEXT
Target Delimiter: comma (“, ”)
Compression: N/A

In Scala using Data Frames and SQL
 */

object Exercise2_DF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Exercise2_DF").master("yarn-client").getOrCreate()

    val orders = spark.read.csv("/data/retail_db/orders")
      .select("_c0", "_c2")
      .withColumnRenamed("_c0", "order_id")
      .withColumnRenamed("_c2", "order_customer_id")

    val customers = spark.read.csv("/data/retail_db/customers")
        .select("_c0", "_c1", "_c2")
        .withColumnRenamed("_c0", "customer_id")
        .withColumnRenamed("_c1", "customer_lname")
        .withColumnRenamed("_c2", "customer_fname")

    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")

    val inactive_customer = spark.sql(
      """
        SELECT DISTINCT CONCAT(customer_lname, ",", customer_fname) as name FROM customers
        LEFT JOIN orders ON order_customer_id = customer_id
        WHERE order_customer_id IS NULL
        ORDER BY name
      """
    )

    inactive_customer
        .coalesce(1)
        .write
        .text("/user/sandbox/solutions_sql/solutions02/inactive_customers")
  }
}
