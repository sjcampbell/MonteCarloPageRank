package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

object TrainSpamClassifier {
    
    val delta = 0.002
    
    val log = Logger.getLogger(getClass().getName())
    val lineParser = new LineParser()
    
    def main(argv: Array[String]) {
        val args = new TrainConf(argv)
        log.info("Input: " + args.input())
        log.info("Model: " + args.model())
        
        val shuffle = args.shuffle.isSupplied && args.shuffle()
        log.info("Shuffle: " + shuffle)
        
        val conf = new SparkConf().setAppName("A6 - Spam Classifier")
        val sc = new SparkContext(conf)
        sc.setJobDescription("Training spam classifier using stochastic gradient descent.")
        
        val modelDir = new Path(args.model())
		FileSystem.get(sc.hadoopConfiguration).delete(modelDir, true)
		
        runSparkJob(sc, args.input(), args.model(), shuffle)
    }
    
    def runSparkJob(sc: SparkContext, input: String, model: String, shuffle: Boolean) {
        val textFile = sc.textFile(input)
        
        if (shuffle) {
            val trained = textFile.map(lineParser.parseDataLineRandomKey)
                .sortByKey()
                .map {
                    case (keyInstance) => {
                        (0, (keyInstance._2._1, keyInstance._2._2, keyInstance._2._3))
                    }
                }
                .groupByKey(1)
                .flatMap(buildModelWeights)
                .saveAsTextFile(model)
        }
        else {
            val trained = textFile.map(parseLine)
            .groupByKey(1)
            .flatMap(buildModelWeights)
            .saveAsTextFile(model)    
        }
    }
    
    def parseLine(line: String): (Int, (String, Double, Array[Int])) = {
        val split = line.split(" ")
        val docid = split(0)
        val isSpam = if (split(1) == "spam") 1.0 else 0.0
        
        val features = split.drop(2).map(f => f.toInt)
        
        (0, (docid, isSpam, features))
    }
    
    def buildModelWeights(keyedInstances: (Int, Iterable[(String, Double, Array[Int])])): TraversableOnce[(Int, Double)] = {
        var weights = Map[Int, Double]()
        keyedInstances._2.foreach {
            case (docid, isSpam, features) => {
                val score = spamminess(features, weights)
                val prob = 1.0 / (1.0 + Math.exp(-score))
                features.foreach(f => {
                    if (weights.contains(f)) {
                        weights = weights.updated(f, (weights(f) + (isSpam - prob) * delta))
                    }
                    else {
                        weights += (f -> (isSpam - prob) * delta)
                    }
                })
            }
        }
        
        weights
    }
    
    def spamminess(features: Array[Int], weights: Map[Int, Double]) : Double = {
        var score = 0d
        features.foreach(f => if (weights.contains(f)) score += weights(f))
        score
    }
}