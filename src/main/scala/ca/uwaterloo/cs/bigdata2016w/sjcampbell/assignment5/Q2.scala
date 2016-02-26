package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

/*
 * Which clerks were responsible for processing items that were shipped on a particular date? List the first 20 by order key.
 * 
 * 	select 
 * 		o_clerk, o_orderkey from lineitem, orders
 * 	where
 * 		l_orderkey = o_orderkey and
 * 		l_shipdate = 'YYYY-MM-DD'
 * 	order by o_orderkey asc limit 20;
 * 
 */
object Q2 {
    val log = Logger.getLogger(getClass().getName())
    
    def main(argv: Array[String]) {
        val args = new Conf(argv)
        log.info("Input: " + args.input())
        log.info("Date: " + args.date())
        log.info("Executors: " + args.numExecutors())
        
        val conf = new SparkConf().setAppName("Q2 - Clerk order items")
        val sc = new SparkContext(conf)
        sc.setJobDescription("Clerks responsible for processing items that were shipped on a particular date")
        
        val lineItems = sc.textFile(args.input() + "/lineitem.tbl")
        val orders = sc.textFile(args.input() + "/orders.tbl")
        val date = args.date()
        
        val outputDir = new Path("q2-output")
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
        
        // First, build each PairRDD with the keys required for the reduce-side (cogroup) join
        val lineItemKeyed = lineItems.map(line => {
            val lineItemRow = line.split("\\|")
            (lineItemRow(0).toInt, lineItemRow(10))   
        })
        
        val ordersKeyed = orders.map(line => {
            val ordersRow = line.split("\\|")
            (ordersRow(0).toInt, ordersRow(6))
        })

        ordersKeyed.cogroup(lineItemKeyed)
        .flatMapValues {
            case (clerks, shipdates) => {
                var clerkdates = List[String]()

                for (clerk <- clerks; shipdate <- shipdates) {
                    if (shipdate.startsWith(date)) {
                        clerkdates = clerk +: clerkdates 
                    }
                }
                
                clerkdates
            }
        }
        .sortByKey()
        .take(20)
        .foreach(clerkOrder => {
              println((clerkOrder._2, clerkOrder._1))
        })
    }
}