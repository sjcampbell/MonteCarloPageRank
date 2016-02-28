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
            (lineItemRow(LineItemColumns.quantity).toDouble,
            lineItemRow(LineItemColumns.extendedPrice).toDouble,
            lineItemRow(LineItemColumns.discount).toDouble,
            lineItemRow(LineItemColumns.tax).toDouble,
            lineItemRow(LineItemColumns.shipDate)))
        })
        .map {
            case ((returnFlag, lineStatus), (quantity, extendedPrice, discount, tax, shipDate)) => {
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
                        calculation.quantitySum, 
                        calculation.basePriceSum,
                        calculation.discountCalc,
                        calculation.chargeCalc,
                        calculation.quantitySum / calculation.count,
                        calculation.basePriceSum / calculation.count,
                        calculation.discountSum / calculation.count,
                        calculation.count))
            }
        }
    }
}

class Q6Calculation(var quantity: Double, var basePrice: Double, var discount: Double, var tax: Double) {
    var count = 1
    var discountCalc = basePrice * (1.0 - discount)
    var chargeCalc = basePrice * (1.0 - discount) * (1.0 + tax)
    var discountSum = discount
    var basePriceSum = basePrice
    var quantitySum = quantity

    def add(add: Q6Calculation) {
        count = count + add.count
        quantitySum = quantitySum + add.quantitySum
        basePriceSum = basePriceSum + add.basePriceSum
        tax = tax + add.tax
        discountSum = discountSum + add.discountSum
        discountCalc = discountCalc + add.discountCalc
        chargeCalc = chargeCalc + add.chargeCalc
    }
}