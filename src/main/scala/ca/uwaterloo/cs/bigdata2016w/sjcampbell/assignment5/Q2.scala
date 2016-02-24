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
        
        // First step, build each PairRDD with the keys required for the reduce-side (cogroup) join
        var lineItemKeyed = lineItems.map(line => {
            val lineItemRow = line.split("\\|")
            (lineItemRow(0), lineItemRow(10))   
        })
        
        val ordersKeyed = orders.map(line => {
            val ordersRow = line.split("\\|")
            (ordersRow(0), ordersRow(6))
        })

        ordersKeyed.cogroup(lineItemKeyed)
        .flatMapValues {
            case (clerks, shipdates) => {
                var clerkdates = List[(String, String)]()

                // TODO: Is there a cleaner syntax?
                //for (c <- clerks.iterator; sd <- shipdates.iterator) yield (c, sd)
                clerks.foreach { 
                    clerk => {
                        shipdates.foreach {
                            shipdate => {
                                if (shipdate.startsWith(date)) {
                                    clerkdates = (clerk, shipdate) +: clerkdates
                                }
                            }
                        }
                    }
                }
                
                clerkdates
            }
        }
        .saveAsTextFile("q2-output")

        /*val result = joinAndGetTop(20, ordersKeyed, lineItemKeyed)
        def joinAndGetTop(topn: Int, rdd1: RDD[(String, String)], rdd2: RDD[(String, String)]) : Array[(String, (String, String))] = {
            rdd1.cogroup(rdd2)
            .flatMapValues {
                case (clerks, shipdates) => {
                    for (c <- clerks.iterator; sd <- shipdates.iterator) yield (c, sd)
                }
            }
            .saveAsTextFile("q2-output")
            .take(topn)
        }*/
        
        // RDD[key: String, clerks: Iterable[String], shipdates: Iterable[String] - Now filter based on date.
        /* This filter looks through whole list of shipdates for a clerk.
        .filter {
            case (key, (clerks, shipdates)) => {
                if (shipdates != null && shipdates.exists { sd => sd.startsWith(date) }) true
                else false
            }
        }
        */
    }
}