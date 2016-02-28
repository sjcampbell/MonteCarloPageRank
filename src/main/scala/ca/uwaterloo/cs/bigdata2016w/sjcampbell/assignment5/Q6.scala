package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

/*
 * Q6 - Pricing summary report
 * 
 * 	select
 * 		l_returnflag,
 *  	l_linestatus,
 *  	sum(l_quantity) as sum_qty,
 *  	sum(l_extendedprice) as sum_base_price,
 *  	sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
 *  	sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
 *  	avg(l_quantity) as avg_qty,
 *  	avg(l_extendedprice) as avg_price,
 *  	avg(l_discount) as avg_disc,
 * 		count(*) as count_order
 * 	from 
 * 		lineitem
 * 	where
 * 		l_shipdate = 'YYYY-MM-DD'
 * 	group by l_returnflag, l_linestatus;
 */
object Q6 {
    val log = Logger.getLogger(getClass().getName())
    
    def main(argv: Array[String]) {
        val args = new Conf(argv)
        log.info("Input: " + args.input())
        log.info("Date: " + args.date())
        log.info("Executors: " + args.numExecutors())
        
        val conf = new SparkConf().setAppName("Q6 - Monthly shipments")
        conf.registerKryoClasses(Array(classOf[Q6Calculation]))
        val sc = new SparkContext(conf)
        sc.setJobDescription("Reports the amount of business that was billed, shipped, and returned")
        
        val date = args.date()
        
        val outputDir = new Path("q6-output")
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
        
        val lineItems = sc.textFile(args.input() + "/lineitem.tbl")
        lineItems.map(line => {
            val lineItemRow = line.split("\\|")
            ((lineItemRow(LineItemColumns.returnFlag), lineItemRow(LineItemColumns.lineStatus)),
            (lineItemRow(LineItemColumns.quantity).toFloat,
            lineItemRow(LineItemColumns.extendedPrice).toFloat,
            lineItemRow(LineItemColumns.discount).toFloat,
            lineItemRow(LineItemColumns.tax).toFloat,
            lineItemRow(LineItemColumns.shipDate), 1))
        })
        .map {
            case ((returnFlag, lineStatus), (quantity, extendedPrice, discount, tax, shipDate, count)) => {
                if (shipDate.startsWith(date)) {
                    ((returnFlag, lineStatus), new Q6Calculation(quantity, extendedPrice, discount, tax))
                }
                else null
            }
        }
        .filter(_ != null)
        .reduceByKey {
            case (calc1, calc2) => {
                calc1.add(calc2)
                calc1
            }
        }
        .collect()
        .foreach {
            case (((returnFlag, lineStatus), calculation)) => {
                println((returnFlag, 
                        lineStatus, 
                        calculation.quantity, 
                        calculation.basePrice,
                        calculation.discPriceSum,
                        calculation.chargeSum,
                        calculation.quantity / calculation.count,
                        calculation.basePrice / calculation.count,
                        calculation.discPriceSum / calculation.count,
                        calculation.count))
            }
        }
    }
}

class Q6Calculation(var quantity: Float, var basePrice: Float, var discount: Float, var tax: Float) {
    var count = 1
    var discPriceSum = 0f
    var chargeSum = 0f
    
    def add(add: Q6Calculation) {
        count = count + add.count
        quantity = quantity + add.quantity
        basePrice = basePrice + add.basePrice
        discount = discount + add.discount
        tax = tax + add.tax
        
        discPriceSum = discPriceSum + (add.basePrice * (1 - add.discount))
        chargeSum = chargeSum + (add.basePrice * (1 - add.discount) * (1 + add.tax))
    }
}

