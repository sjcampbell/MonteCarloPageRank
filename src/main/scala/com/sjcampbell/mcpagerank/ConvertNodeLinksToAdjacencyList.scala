package com.sjcampbell.mcpagerank

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

/*
 *  Utility to convert pairwise graph nodes into adjacency list
 */
object ConvertPairsToAdjacencyList {
    val log = Logger.getLogger(getClass().getName())
    
    def parseLine(line: String) : (Int, Int) = {
        val parts = line.split("\\s+")        
        (parts(0).toInt, parts(1).toInt)
    }
    
    def deleteOutputFile(path: String, sc: SparkContext) = {
        val outputDir = new Path(path)
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    }    
    
    def main(argv: Array[String]) {
        val args = new Conf(argv)
        val nodeCount = args.nodeCount()
        log.info("Input: " + args.input())
        log.info("Number of Nodes: " + nodeCount)
        
        val conf = new SparkConf().setAppName("MonteCarloPageRank")
        val sc = new SparkContext(conf)
        sc.setJobDescription("Takes a file that describes a graph by one link on each line and converts it into an adjacency list.")
        
        val output = args.input().split("\\.").dropRight(1).mkString + "_adjacencyList"
        deleteOutputFile(output, sc)
        
        // Parse input adjacency list into (nodeID, Array[nodeId])
        val adjList = sc.textFile(args.input())
        .map(parseLine)
        .groupByKey()
        .map {
            case (source, destinations) => {
                source + "\t" + destinations.mkString("\t")
            }
        }
        .saveAsTextFile(output)
    }
}