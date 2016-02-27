package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

/*
 * How many items were shipped to each country on a particular date?
 * 
 * 	select n_nationkey, n_name, count(*) from lineitem, orders, customer, nation
 * 	where
 *   	l_orderkey = o_orderkey and
 *     	o_custkey = c_custkey and
 *     	c_nationkey = n_nationkey and
 *     	l_shipdate = 'YYYY-MM-DD'
 * 	group by n_nationkey, n_name
 * 	order by n_nationkey asc;
 */
object Q4 {
    val log = Logger.getLogger(getClass().getName())
    
    def getFilteredLineItems(dir: String, sc: SparkContext, shipDateFilter: String) = {
        val lineItems = sc.textFile(dir + "/lineitem.tbl")
        lineItems.map(line => {
            val lineItemRow = line.split("\\|")

            // orderKey, shipDate
            (lineItemRow(0).toInt, lineItemRow(10))
        })
        .filter(pair => pair._2.startsWith(shipDateFilter))
    }
    
    def getOrdersKeyed(dir: String, sc: SparkContext) = {
        val orders = sc.textFile(dir + "/orders.tbl")
        orders.map(line => {
            val orderRow = line.split("\\|")

            // orderKey, customerKey
            (orderRow(0).toInt, orderRow(1).toInt)
        })
    }
    
    def getCustomersKeyed(dir: String, sc: SparkContext) = {
        val customers = sc.textFile(dir + "/customer.tbl")
        customers.map(line => {
            val customerRow = line.split("\\|")

            // customerKey, nationKey
            (customerRow(0).toInt, customerRow(3).toInt)
        })
    }
    
    def getNationsKeyed(dir: String, sc: SparkContext) = {
        val nations = sc.textFile(dir + "/nation.tbl")
        nations.map(line => {
            val nationRow = line.split("\\|")

            // nationKey, nationName
            (nationRow(0).toInt, nationRow(1))   
        })
    }
    
    def main(argv: Array[String]) {
        val args = new Conf(argv)
        log.info("Input: " + args.input())
        log.info("Date: " + args.date())
        log.info("Executors: " + args.numExecutors())
        
        val conf = new SparkConf().setAppName("Q4 - Shipped to country on date")
        val sc = new SparkContext(conf)
        sc.setJobDescription("How many items were shipped to each country on a particular date?")
        
        val date = args.date()
        val customersKeyed = getCustomersKeyed(args.input(), sc)
        val nationsKeyed = getNationsKeyed(args.input(), sc)
        
        // First join nation and customer in-memory        
        val nationsMap = nationsKeyed.collectAsMap()
        val customerNation = customersKeyed.collectAsMap().map {
            case (customerKey, nationKey) => {
                (customerKey, (nationKey, nationsMap.get(nationKey).get))
            }
        }
        
        val custNationMap = sc.broadcast(customerNation)

        // Join orders with lineitems and lookup nation by using the customer key from the orders row
        val ordersKeyed = getOrdersKeyed(args.input(), sc)
        val lineItemsKeyed = getFilteredLineItems(args.input(), sc, date)
        lineItemsKeyed.cogroup(ordersKeyed)
        .flatMap {
            case (orderKey, (shipDates, customerKeys)) => {
                var nationCounts = List[((Int, String), Int)]()
                
                for (custKey <- customerKeys; shipDate <- shipDates) {
                    val nation = custNationMap.value.get(custKey).get
                    nationCounts = ((nation._1, nation._2), 1) +: nationCounts
                }
                
                nationCounts
            }
        }
        .reduceByKey(_ + _)  // Sum counts for each nation 
        .sortByKey()
        .collect()
        .foreach {
            case ((nationKey, nationName), count) => {
                println(nationKey, nationName, count)
            }
        }
    }
}