package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

/*
 * 	select 
 * 		n_nationkey, n_name, to_char(l_shipdate, 'YYYY-MM'), count(*) 
 * 	from 
 * 		lineitem, orders, customer, nation 
 * 	where
 *   	l_orderkey = o_orderkey and
 *   	o_custkey = c_custkey and
 *   	c_nationkey = n_nationkey and
 *   	(n_nationkey = 3 or n_nationkey = 24)
 *   group by 
 *   	n_nationkey, to_char(l_shipdate, 'YYYY-MM')
 *   order by 
 *   	n_nationkey, to_char(l_shipdate, 'YYYY-MM');
 */
object Q5 {
    val log = Logger.getLogger(getClass().getName())
    
    def getKeyedLineItems(dir: String, sc: SparkContext) = {
        val lineItems = sc.textFile(dir + "/lineitem.tbl")
        lineItems.map(line => {
            val lineItemRow = line.split("\\|")

            // orderKey, shipDate
            (lineItemRow(0).toInt, lineItemRow(10))
        })
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
        log.info("Executors: " + args.numExecutors())
        
        val conf = new SparkConf().setAppName("Q5 - Monthly shipments")
        val sc = new SparkContext(conf)
        sc.setJobDescription("Find monthly shipment data to Canada and United States.")

        val customersKeyed = getCustomersKeyed(args.input(), sc)
        val nationsKeyed = getNationsKeyed(args.input(), sc)
        
        // First join nation and customer in-memory        
        val nationsMap = nationsKeyed.collectAsMap()
        val customerNation = customersKeyed.collectAsMap().map {
            case (customerKey, nationKey) => {
                (customerKey, (nationKey, nationsMap.get(nationKey).get))
            }
        }
        
        val canadaId = 3
        val unitedStatesId = 24
        val custNationMap = sc.broadcast(customerNation)

        // Join orders with lineitems and lookup nation by using the customer key from the orders row
        val ordersKeyed = getOrdersKeyed(args.input(), sc)
        val lineItemsKeyed = getKeyedLineItems(args.input(), sc)
        lineItemsKeyed.cogroup(ordersKeyed)
        
        // Generate RDD[(nationId, month(shipdate)), 1]
        .flatMap {
            case (orderKey, (shipDates, customerKeys)) => {
                var shippedCounts = List[((String, String), Int)]()
                
                for (custKey <- customerKeys; shipDate <- shipDates) {
                    val nation = custNationMap.value.get(custKey).get
                    if (nation._1 == canadaId || nation._1 == unitedStatesId) {
                        shippedCounts = ((nation._2, shipDate.substring(0, 7)), 1) +: shippedCounts   
                    }
                }
                
                shippedCounts
            }
        }
        
        // Reduce to sum the number of shipments, grouped by country and month.
        .reduceByKey(_ + _)
        .sortByKey()
        .collect()
        .foreach {
            case ((nationName, shipDate), count) => {
                println(nationName, shipDate, count)
            }
        }
    }
}