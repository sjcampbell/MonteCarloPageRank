package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

object ApplySpamClassifier {
    
    val log = Logger.getLogger(getClass().getName())
    
    def main(argv: Array[String]) {
        val args = new ApplyConf(argv)
        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Model: " + args.model())
        
        val conf = new SparkConf().setAppName("A6 - Apply Spam Classifier")
        val sc = new SparkContext(conf)
        sc.setJobDescription("Classifies a document as spam or ham.")
        
        val outputDir = new Path(args.output())
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
        
        runSparkJob(sc, args.input(), args.output(), args.model())
    }
    
    def runSparkJob(sc: SparkContext, input: String, output: String, model: String) {

        val weights = sc.textFile(model).map(parseModelLine).collectAsMap()
        
        val testData = sc.textFile(input).map(parseInputLine)
        
        testData.map {
            case (docid, isSpam, features) => {
                val sp = spamminess(features, weights)
                val predictedSpam = if (sp > 0) "spam" else "ham"
                (docid, isSpam, sp, predictedSpam)
            }
        }
        .saveAsTextFile(output)
    }
    
    def parseInputLine(line: String) : (String, Int, Array[Int]) = {
        val split = line.split(" ")
        val docid = split(0)
        val isSpam = if (split(1) == "spam") 1 else 0
        
        val features = split.drop(2).map(f => f.toInt)
        
        (docid, isSpam, features)
    }
    
    def parseModelLine(line: String) : (Int, Double) = {
        val split = line.split("[\\(,\\)]")

        // feature, weight 
        (split(1).toInt, split(2).toDouble)
    }
    
    def spamminess(features: Array[Int], weights: scala.collection.Map[Int, Double]) : Double = {
        var score = 0d
        features.foreach(f => if (weights.contains(f)) score += weights(f))
        score
    }
}