import org.apache.spark.{SparkContext, SparkConf}

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

In Scala using Core API
 */

object Exercise2 {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Exercise2").setMaster("yarn-client")
    val sc = new SparkContext(conf)

    val orders = sc.textFile("/data/retail_db/orders")
    val customers = sc.textFile("/data/retail_db/customers")

    val orders_map = orders.map(order => {
      val order_split = order.split(",")
      (order_split(2).toInt, order_split(2).toInt)
    })

    val customer_map = customers.map(customer => {
      val customer_split = customer.split(",")
      (customer_split(0).toInt, (customer_split(1), customer_split(2)))
    })

    val inactive_customer = customer_map
                          .leftOuterJoin(orders_map)
                          .cache()
                          .filter(customer => customer._2._2 == None)
                          .map(customer => ((customer._2._1._1, customer._2._1._2),
                                  (customer._2._1._1 + ", " + customer._2._1._2)))
                          .distinct()
                          .sortByKey()

    inactive_customer
      .map(customer => customer._2)
      .saveAsTextFile("/user/sandbox/solutions/solutions02/inactive_customers")

  }
}
