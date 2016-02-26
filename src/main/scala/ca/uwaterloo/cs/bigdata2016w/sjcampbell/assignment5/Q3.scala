package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

/*
 * What are the names of parts and suppliers of items shipped on a particular date?
 * List the first 20 by order key.
 * 
 * 	select l_orderkey, p_name, s_name from lineitem, part, supplier
 * 	where
 *   l_partkey = p_partkey and
 *   l_suppkey = s_suppkey and
 *   l_shipdate = 'YYYY-MM-DD'
 * order by l_orderkey asc limit 20;
 */
object Q3 {
    val log = Logger.getLogger(getClass().getName())
    
    def getLineItemsKeyed(dir: String, sc: SparkContext) = {
        val lineItems = sc.textFile(dir + "/lineitem.tbl")
        lineItems.map(line => {
            val lineItemRow = line.split("\\|")

            // orderKey, (partKey, suppKey, shipDate)
            (lineItemRow(0).toInt, (lineItemRow(1).toInt, lineItemRow(2).toInt, lineItemRow(10)))   
        })
    }
    
    def getPartsKeyed(dir: String, sc: SparkContext) = {
        val parts = sc.textFile(dir + "/part.tbl")
        parts.map(line => {
            val partRow = line.split("\\|")
            
            // partKey, partName
            (partRow(0).toInt, partRow(1))
        })
    }
    
    def getSuppliersKeyed(dir: String, sc: SparkContext) = {
        val suppliers = sc.textFile(dir + "/supplier.tbl")
        suppliers.map(line => {
            val supplierRow = line.split("\\|")
            
            // supplierKey, supplierName
            (supplierRow(0).toInt, supplierRow(1))
        })
    }
  
    def main(argv: Array[String]) {
        val args = new Conf(argv)
        log.info("Input: " + args.input())
        log.info("Date: " + args.date())
        log.info("Executors: " + args.numExecutors())
        
        val conf = new SparkConf().setAppName("Q3 - Part and supplier names")
        val sc = new SparkContext(conf)
        sc.setJobDescription("What are the names of parts and suppliers of items shipped on a particular date?")

        // First, build each PairRDD with the keys required for the join
        val lineItemsKeyed = getLineItemsKeyed(args.input(), sc)
        val partsKeyed = getPartsKeyed(args.input(), sc)
        val suppliersKeyed = getSuppliersKeyed(args.input(), sc)
        
        val partsBroadcast = sc.broadcast(partsKeyed.collectAsMap())
        val suppliersBroadcast = sc.broadcast(suppliersKeyed.collectAsMap())
        
        val date = args.date()
        
        val result = lineItemsKeyed.map {
            case (orderKey, (partKey, suppKey, shipDate)) => {
                val partName = partsBroadcast.value.get(partKey)
                val supplierName = suppliersBroadcast.value.get(suppKey)
                (orderKey, (partName, supplierName, shipDate))
            }
        }
        .filter{
            case (orderKey, (partKey, suppKey, shipDate)) => {
                shipDate.startsWith(date)
            }
        }
        .sortByKey()
        .take(20)
        .foreach {
            case (orderKey, (partName, supplierName, shipDate)) => {
                println((orderKey, partName.get, supplierName.get))
            }
        }
    }
}