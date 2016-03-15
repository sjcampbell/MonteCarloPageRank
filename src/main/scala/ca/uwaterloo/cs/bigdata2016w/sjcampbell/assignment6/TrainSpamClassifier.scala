package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

object TrainSpamClassifier {
    
    val delta = 0.2
    
    val log = Logger.getLogger(getClass().getName())
    
    def main(argv: Array[String]) {
        val args = new Conf(argv)
        log.info("Input: " + args.input())
        log.info("Model: " + args.model())
        log.info("Executors: " + args.numExecutors())
        
        val conf = new SparkConf().setAppName("A6 - Spam Classifier")
        val sc = new SparkContext(conf)
        sc.setJobDescription("Training spam classifier using stochastic gradient descent.")
        
        val outputDir = new Path(args.model())
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
        
        runSparkJob(sc, args.input(), args.model())
    }
    
    def runSparkJob(sc: SparkContext, input: String, model: String) {
        val textFile = sc.textFile(input)
        val trained = textFile.map(line => { 
            // Parse input
            val split = line.split(" ")
            val docid = split(0)
            val isSpam = if (split(1) == "spam") 1 else 0
            
            val features = split.drop(2).map(f => f.toInt)
            
            (0, (docid, isSpam, features))
        })
        .groupByKey(1)

        trained.flatMap {
            case (key, instances) => {
                var weights = Map[Int, Double]()
                instances.foreach {
                    case (docid, isSpam, features) => {
                        val score = spamminess(features, weights)
                        val prob = 1.0 / (1 + Math.exp(score))
                        features.foreach(f => {
                            if (weights.contains(f)) {
                                weights = weights.updated(f, (weights(f) + (isSpam - prob) * delta))   
                            }
                            else {
                                weights += (f -> 1.0)
                            }
                        })
                    }
                }
                weights
            }
        }        
        .saveAsTextFile(model);
    }
    
    def spamminess(features: Array[Int], weights: Map[Int, Double]) : Double = {
        var score = 0d
        features.foreach(f => if (weights.contains(f)) score += weights(f))
        score
    }
}