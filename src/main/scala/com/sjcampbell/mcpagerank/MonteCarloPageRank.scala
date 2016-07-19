package com.sjcampbell.mcpagerank

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._


/*
 *  Monte Carlo PageRank Algorithm for Spark
 *  
 *  Sam Campbell
 *  University of Waterloo
 *  CS798 Course Project
 *  
 *  This Spark job calculates PageRank for a graph link structure represented as an adjacency list.
 *  It uses a Monte Carlo approach, with random walks, as described in the paper:
 *  "Fast Distributed PageRank", by Das Sarma et al.,
 *  Distributed Computing and Networking Lecture Notes in Computer Science (2013)
 */
object MonteCarloPageRank {
  
    val log = Logger.getLogger(getClass().getName())
    
    // Probability that a surfer will jump to a random node = 0.15
    val randomJump = 0.15f
    val randomSeed = 1234

    def parseLine(line: String) : (Int, Array[Int]) = {
        val parts = line.split("\\s+")
        
        if (parts.length < 2)
            (parts(0).toInt, new Array[Int](0))
        else
            (parts(0).toInt, parts.drop(0).map(_.toInt))
    }
    
    def deleteOutputFile(path: String, sc: SparkContext) = {
        val outputDir = new Path(path)
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    }
    
    def takeRoundOfSteps(adjList: RDD[(Int, Array[Int])], currentCoupons: RDD[(Int, Int)]) : RDD[(Int, Int)] = {
        // MapPartitions is required so that each node can have its own random number generator defined once (per thread), 
        // rather than redefining it for every map iteration.    
        adjList.join(currentCoupons).mapPartitionsWithIndex {
            case (index, iter) => {
                // Use the index of the Spark node as a part of the random seed, so that Spark nodes
                // don't all generate the same random numbers.
                val rand = new scala.util.Random(randomSeed + index)
                
                //val distributed = scala.collection.mutable.ListBuffer[(Int, (Int, Int))]()
                var distributed = List[(Int, Int)]() 
                
                iter.foreach {
                    case (nodeId, (neighbours, currentCount)) => {
                        if (neighbours.size > 0) {
                            // Here's where we distribute the walks
                            for (j <- 0 to currentCount) {
                                // Generate random variable to determine if the walk should continue.
                                val d = rand.nextDouble()
                                if (d >= randomJump) {
                                    // Random variable to choose which neighbour the walk should go to next.
                                    val r = rand.nextInt(neighbours.size)
                                    val selectedNeighbour = neighbours(r)

                                    distributed = (selectedNeighbour, 1) +: distributed
                                }
                            }
                        }
                        // Else: There are no neighbours to continue walks from this node, so the walks from this node end.
                    }
                }
                
                distributed.iterator
            }
        }
    }
    
    def main(argv: Array[String]) {
        val args = new McConf(argv)
        val nodeCount = args.nodeCount()
        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Number of Nodes: " + nodeCount)
        log.info("Number of Iterations: " + args.iterations())
        log.info("Number of Random Walks per node: " + args.walks())
        log.info("Executors: " + args.numExecutors())
        
        val conf = new SparkConf().setAppName("MonteCarloPageRank")
        val sc = new SparkContext(conf)
        sc.setJobDescription("Takes an adjacency list and calculates PageRank for graph nodes using a Monte Carlo approach with random walks.")
        deleteOutputFile(args.output(), sc)
        
        // Parse input adjacency list into (nodeID, Array[nodeId])
        val adjList = sc.textFile(args.input()).map(parseLine).cache()

        // There should be at least one walk per node.
        val nodeWalkCount = Math.max(args.walks(), 1)
        
        // Initialize coupons, the term used for the number of random walks at a node.
        // Initialize using a tuple => (<number of walks currently at this node>, <total number of walks that have visited this node>)
        var currentCoupons = adjList.mapValues(v => nodeWalkCount)
        var walkCounts = currentCoupons.cache()

        /*
         * Each iteration will be one walk step for all nodes.
         * We should keep iterating until all walks are complete. The number of iterations just sets a maximum on the loop 
         */
        for (i <- 0 to args.iterations()) {
            log.info("ITERATION: " + i)

            // Move all walks by one step
            val distributedWalks = takeRoundOfSteps(adjList, currentCoupons)
            
            // Reduce to sum the couponCount moving to each node.
            currentCoupons = distributedWalks.reduceByKey(_ + _)

            // Add the new walk steps to the visit totals for each node.
            walkCounts = currentCoupons.union(walkCounts).reduceByKey(_ + _)
        }
        
        // Get total number of walks
        val sum = sc.accumulator(0)
        walkCounts.foreach(wc => sum += wc._2)
        val totalWalks = sum.value
        println("*** Total walk steps (visits) taken: " + totalWalks + " ***")

        // Compute and output PageRanks
        walkCounts.mapValues { x => x.toFloat / totalWalks.toFloat }.sortBy((nodeRank) => { nodeRank._2 }, false, 1).saveAsTextFile(args.output())
    }
}