package com.sjcampbell.mcpagerank

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

object MonteCarloPageRank {
  
    val log = Logger.getLogger(getClass().getName())
    
    def main(argv: Array[String]) {
        val args = new Conf(argv)
        val numnodes = args.numnodes()
        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Number of Nodes: " + numnodes)
        log.info("Executors: " + args.numExecutors())
        
        val conf = new SparkConf().setAppName("BuildPageRankRecords")
        val sc = new SparkContext(conf)
        sc.setJobDescription("Takes an adjacency list and formats them into records that can be used by PageRank.")
        
        val nodeweight = -Math.log(numnodes)
        val adjList = sc.textFile(args.input())
        
        
        
    }
}