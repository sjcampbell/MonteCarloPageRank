package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

/*
 * 	Q7 - Shipping Priority Query
 *	select
 * 		c_name,
 *   	l_orderkey,
 *     	sum(l_extendedprice*(1-l_discount)) as revenue,
 *      o_orderdate,
 *      o_shippriority
 *	from customer, orders, lineitem
 * 	where
 *  	c_custkey = o_custkey and
 *  	l_orderkey = o_orderkey and
 *  	o_orderdate < "1996-01-01" and
 *  	l_shipdate > "1996-01-01"
 *  group by
 *  	c_name,
 *  	l_orderkey,
 *  	o_orderdate,
 *  	o_shippriority
 *  order by
 *  	revenue desc
 *  limit 10;
 */
object Q7 {
    val log = Logger.getLogger(getClass().getName())
    
    def getFilteredLineItems(dir: String, sc: SparkContext, filterDate: String) = {
        val lineItems = sc.textFile(dir + "/lineitem.tbl")
        lineItems.map(line => {
            val lineItemRow = line.split("\\|")
            (lineItemRow(LineItemColumns.orderKey).toInt,
                 (lineItemRow(LineItemColumns.extendedPrice).toDouble,
                     lineItemRow(LineItemColumns.discount).toDouble,
                     lineItemRow(LineItemColumns.shipDate)))
        })
        .filter {
            case (orderKey, (extendedPrice, discount, shipDate)) => {
                shipDate > filterDate
            }
        }
    }
    
    def getFilteredOrders(dir: String, sc: SparkContext, filterDate: String) = {
        val orders = sc.textFile(dir + "/orders.tbl")
        orders.map(line => {
            val orderRow = line.split("\\|")
            (orderRow(OrderColumns.orderKey).toInt, 
                (orderRow(OrderColumns.custKey).toInt, 
                        orderRow(OrderColumns.orderDate), 
                        orderRow(OrderColumns.shipPriority).toInt))
        })
        .filter {
            case (orderKey, (custKey, orderDate, shipPriority)) => {
                orderDate < filterDate
            }
        }
    }
    
    def getCustomersKeyed(dir: String, sc: SparkContext) = {
        val customers = sc.textFile(dir + "/customer.tbl")
        customers.map(line => {
            val customerRow = line.split("\\|")
            (customerRow(CustomerColumns.custKey).toInt, customerRow(CustomerColumns.name))
        })
    }
    
    def main(argv: Array[String]) {
        val args = new Conf(argv)
        log.info("Input: " + args.input())
        log.info("Date: " + args.date())
        log.info("Executors: " + args.numExecutors())
        
        val conf = new SparkConf().setAppName("Q7 - Top 10 unshipped orders")
        val sc = new SparkContext(conf)
        sc.setJobDescription("Retrieves unshipped orders with the 10 highest values")
        
        val outputDir = new Path("q7-output")
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
        
        val date = args.date()
        
        val lineItems = getFilteredLineItems(args.input(), sc, date)
        val orders = getFilteredOrders(args.input(), sc, date)
        
        lineItems.cogroup(orders)
        .flatMap {
            case (orderKey, (lineItemIter, orderIter)) => {

                var rows = List[((Int, Int, String, Int), Double)]()
                
                var revenue = 0.0
                
                for (lineItem <- lineItemIter; order <- orderIter) {

                    // sum(extendedPrice, * (1 - discount))
                    revenue = revenue + lineItem._1 * (1.0 - lineItem._2)
                    
                    // (custKey, orderKey, orderDate, shipPriority), revenue
                    rows = ((order._1, orderKey, order._2, order._3), revenue) +: rows 
                }
                
                rows
            }
        }
        .reduceByKey(_ + _)
        // Sort by revenue, descending and take top 10
        .takeOrdered(10)(Ordering[Double].reverse.on { x => x._2 })
        .foreach {
            case ((custKey, orderKey, orderDate, shipPriority), revenue) => {
                println(custKey, orderKey, revenue, orderDate, shipPriority)
            }
        }
        
        /*
         * 	select
         * 		c_name,
         *   	l_orderkey,
         *     	sum(l_extendedprice*(1-l_discount)) as revenue,
         *      o_orderdate,
         *      o_shippriority
         *	from customer, orders, lineitem
         * 	where
         *  	c_custkey = o_custkey and
         *  	l_orderkey = o_orderkey and
         *  	o_orderdate < "1996-01-01" and
         *  	l_shipdate > "1996-01-01"
         *  group by
         *  	c_name,
         *  	l_orderkey,
         *  	o_orderdate,
         *  	o_shippriority
         *  order by
         *  	revenue desc
         *  limit 10;
         */
    }
}