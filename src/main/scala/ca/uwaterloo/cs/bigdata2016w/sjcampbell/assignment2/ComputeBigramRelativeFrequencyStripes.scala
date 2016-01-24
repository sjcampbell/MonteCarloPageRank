package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment2

import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  
  def mergeMaps(map1: Map[String, Int], map2: Map[String, Int]): Map[String, Int] = {
      map1 ++ map2.map(pair => pair._1 -> (pair._2 + map1.getOrElse(pair._1, 0)))
  }
  
  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency - Stripes")
    val sc = new SparkContext(conf)
    sc.setJobDescription("Computing bigram relative frequency using stripes")
    
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    
    val textFile = sc.textFile(args.input())
    
    textFile.flatMap(line => {
      val tokens = tokenize(line)
      if (tokens.length > 1) {
        tokens.sliding(2).map(p => (p(0), Map((p(1), 1)))) // Iterator[(String, HashMap[String, Int])] 
      }
      else List()
    })
    // RDD[String, HashMap(String, Int)]
    .reduceByKey((ar1, ar2) => {
      mergeMaps(ar1, ar2)
    }, args.reducers())
    // RDD[String, Map(String, Int)]
    .flatMap((line) => {
      val word1 = line._1
      var wordMap = line._2
      val word1Count = wordMap.foldLeft(0)(_+_._2)
      wordMap.map((w2Item) => ((word1, w2Item._1), w2Item._2.toFloat/word1Count.toFloat))
    })
    // RDD[((String, String), Double)]
    .saveAsTextFile(args.output())
  }
}