package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

/*
 * How many items were shipped on a particular date?
 * select count(*) from lineitem where l_shipdate = 'YYYY-MM-DD';
 * 
 * Terminal alternative
 * cat input/lineitem.tbl | awk -F'|' '{print $11}' | grep '1996-01-01' | wc -l
 */
object Q1 extends {
    val log = Logger.getLogger(getClass().getName())
    
    def main(argv: Array[String]) {
        val args = new Conf(argv)
        log.info("Input: " + args.input())
        log.info("Date: " + args.date())
        log.info("Executors: " + args.numExecutors())
        
        val conf = new SparkConf().setAppName("Q1 - Count shipped items")
        val sc = new SparkContext(conf)
        sc.setJobDescription("Count how many items where shipped on a particular date.")
        
        var itemCount = sc.accumulator(0)
        val lineItems = sc.textFile(args.input() + "/lineitem.tbl")
        val date = args.date()
        
        lineItems.map(line => {
            val shipdate = line.split("\\|")(10)
            if (shipdate.startsWith(date)) {
                itemCount += 1
            }
        })
        
        println("ANSWER=" + itemCount)
    }
}