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
            
            val shipDate = lineItemRow(LineItemColumns.shipDate)
            if (shipDate > filterDate) {
                val extendedPrice = lineItemRow(LineItemColumns.extendedPrice).toDouble
                val discount = lineItemRow(LineItemColumns.discount).toDouble
                (lineItemRow(LineItemColumns.orderKey).toInt, (extendedPrice, discount, extendedPrice*(1.0 - discount)))
            }
            else null
        })
        .filter(_ != null)
    }
    
    def getFilteredOrders(dir: String, sc: SparkContext, filterDate: String) = {
        val orders = sc.textFile(dir + "/orders.tbl")
        orders.map(line => {
            val orderRow = line.split("\\|")
            val orderDate = orderRow(OrderColumns.orderDate)
            if (orderDate < filterDate) {
                (orderRow(OrderColumns.orderKey).toInt, (orderDate,  orderRow(OrderColumns.shipPriority)))
            }
            else null
        })
        .filter(_ != null)
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
        
        val date = args.date()
        
        val lineItems = getFilteredLineItems(args.input(), sc, date)
        val orders = getFilteredOrders(args.input(), sc, date)
        
        orders.cogroup(lineItems)
        .flatMap {
            case (orderKey, (lineItemIter, orderIter)) => {
               
                // TODO...
                for (lineItem <- lineItemIter; order <- orderIter) {
                    null
                }
                
                null
            }
        }
        
        
    }
}