package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment2

import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val imc = opt[Boolean](descr = "use in-mapper combining", required = false)
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def bigramCountIter(lines: Iterator[String]) : Iterator[((String, String), Int)] = {
      var res = List[((String, String), Int)]()
      
      for (line <- lines) {
        val tokens = tokenize(line)
        
        if (tokens.length > 1) {
          val window = tokens.sliding(2)  // Iterator[List[String]]
          for (words <- window) {
            res :+ ((words(0), words(1), 1))
            res :+ ((words(0), "*", 1))
          }
        }
      }
          
      res.iterator
    }
  
  def bigramIter(lines: Iterator[String]) : Iterator[(String, String)] = {
      var res = List[(String, String)]()
      
      for (line <- lines) {
        val tokens = tokenize(line)
        
        if (tokens.length > 1) {
          val window = tokens.sliding(2)  // Iterator[List[String]]
          for (words <- window) {
            res :+ (words(0), words(1))
            res :+ (words(0), "*")
          }
        }
      }
          
      res.iterator
    }

  // Partition based on first word in key of form (String, String) 
  class FirstWordPartitioner(numParts: Int) extends Partitioner {
    def numPartitions: Int = numParts

    def getPartition(key: Any): Int = {
      val pair = key.asInstanceOf[(String, String)]
      (pair._1.hashCode() & Int.MaxValue) % numParts
    }
  }
  
  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency - Pairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
/*
    // Approach 0
    // ==========
    
    var sum = 0;
    textFile.flatMap (line => {
      val tokens = tokenize(line)
      if (tokens.length > 1) tokens.sliding(2).map(p => ((p(0), p(1)), 1)).toList else List()
    })
    
    // Returns: RDD[(String, String), Int]  Next: Combine/reduce
    .reduceByKey(new FirstWordPartitioner(args.reducers()), _ + _)

    // Returns: RDD[(String, String), Int]  Next: Map to calculate for each pair
    .map((pairCount) => {
      if (pairCount._1.asInstanceOf[(String,String)]._2 == "*") {
        sum = pairCount._2
      }
      else {
        (pairCount._1, pairCount._2 / sum)  
      }
    })
    .saveAsTextFile(args.output())
    
    println("!!! Job Completed !!!")*/

    // Approach 1
    // ==========
    
    textFile.flatMap (line => {
      val tokens = tokenize(line)
      if (tokens.length > 1) {
        tokens.sliding(2).flatMap(p => List(((p(0), p(1)), 1), ((p(0), "*"), 1))) 
      }
      else List()
    })
    
    // Returns: RDD[(String, String), Int]  Next: Combine/reduce
    .reduceByKey(new FirstWordPartitioner(args.reducers()), _ + _)
    
    // TODO: Is this needed?
    .repartitionAndSortWithinPartitions(new FirstWordPartitioner(args.reducers()))
    // Returns: RDD[(String, String), Int]  Next: Map to calculate for each pair
    .mapPartitions(pairCounts => {
        // On a partition that now has grouped keys...
      var sum = 0;
      var res = List[((String, String), Int)]()
      
      var count = 0;
      
      for (pairCount <- pairCounts) {
        
        count += 1
        
        if (pairCount._1.asInstanceOf[(String, String)]._2 == "*"){
          sum = pairCount._2 
        }
        else {
          if (sum == 0) println("WARNING: Sum was ZERO when attempting to calculate relative frequency.")
          // If sum==0, make sure things are grouped and ordered properly.
            
          count = pairCount._2
          res.:: (pairCount._1, count / sum)
        }
      }
      
      println("Iterated through all pairCounts. There were " + count);
      
      res.iterator
    })
    /* TODO: Not used
    .map((pairCount) => {
      if (pairCount._1.asInstanceOf[(String,String)]._2 == "*") {
        sum = pairCount._2
      }
      else {
        (pairCount._1, pairCount._2 / sum)  
      }
    })*/
    .saveAsTextFile(args.output())
    
    println("!!! Job Completed !!!")

  }
}