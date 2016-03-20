package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

object ApplyEnsembleSpamClassifier {
    val log = Logger.getLogger(getClass().getName())
    val spamClassifier = new SpamClassifier()
    
    def main(argv: Array[String]) {
        val args = new ApplyEnsembleConf(argv)
        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Model: " + args.model())
        log.info("Method: " + args.method())
        
        val conf = new SparkConf().setAppName("A6 - Apply Ensemble Spam Classifier")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)
        sc.setJobDescription("Classifies a document as spam or ham using ensemble techniques.")
        
        val outputDir = new Path(args.output())
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
        
        runSparkJob(sc, args.input(), args.output(), args.model(), args.method())
    }
    
    def runSparkJob(sc: SparkContext, input: String, output: String, modelDir: String, method: String) {
        val weights0 = sc.textFile(modelDir + "/part-00000").map(parseModelLine).collectAsMap()
        val weights1 = sc.textFile(modelDir + "/part-00001").map(parseModelLine).collectAsMap()
        val weights2 = sc.textFile(modelDir + "/part-00002").map(parseModelLine).collectAsMap()
        
        val bcWeights0 = sc.broadcast(weights0)
        val bcWeights1 = sc.broadcast(weights1)
        val bcWeights2 = sc.broadcast(weights2)
        
        val testData = sc.textFile(input).map(parseDataLine)
        
        testData.map {
            case (docid, isSpam, features) => {
                val sp0 = spamClassifier.spamminess(features, bcWeights0.value)
                val sp1 = spamClassifier.spamminess(features, bcWeights1.value)
                val sp2 = spamClassifier.spamminess(features, bcWeights2.value)
                
                if (method == "average") {
                    val classified = spamClassifier.EnsembleAverage(sp0, sp1, sp2)
                	(docid, isSpam, classified._1, classified._2)
                }
                // Default to "vote" ensemble method
                else {
                    val classified = spamClassifier.EnsembleVote(sp0, sp1, sp2)
                    (docid, isSpam, classified._1, classified._2)
                }
            }
        }
        .saveAsTextFile(output)
    }
    
    def spamminess(features: Array[Int], weights: scala.collection.Map[Int, Double]) : Double = {
        var score = 0d
        features.foreach(f => if (weights.contains(f)) score += weights(f))
        score
    }
    
    def parseDataLine(line: String) : (String, String, Array[Int]) = {
        val split = line.split(" ")
        val docid = split(0)
        val isSpam = split(1)
        
        val features = split.drop(2).map(f => f.toInt)
        
        (docid, isSpam, features)
    }
    
    def parseModelLine(line: String) : (Int, Double) = {
        val split = line.split("[\\(,\\)]")

        // feature, weight 
        (split(1).toInt, split(2).toDouble)
    }
}