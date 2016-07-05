package com.sjcampbell.mcpagerank

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._


/* Power Iteration PageRank Algorithm for Spark
 * 
 *  Sam Campbell
 *  University of Waterloo
 *  CS798 Course Project
 *  
 *  See Hadoop MapReduce implementation of this PageRank algorithm (Note how verbose it is compared to Spark) 
 *  https://github.com/lintool/bespin/blob/master/src/main/java/io/bespin/java/mapreduce/pagerank/RunPageRankBasic.java
 */
object PowerIterationPageRank {
  
    val log = Logger.getLogger(getClass().getName())
    
    // Probability that a surfer will jump to a random node = 0.15
    val randomJump = 0.15f
    
    def parseLine(line: String) : (Int, Array[Int]) = {
        val parts = line.split("\\s+")
        
        if (parts.length < 2)
            (parts(0).toInt, new Array[Int](0))
        else
            (parts(0).toInt, parts.drop(0).map(_.toInt))
    }
    
    // Adds two log probs.
    def sumLogProbs(a: Float, b: Float) : Float = {
        if (a == Float.NegativeInfinity)
          return b;
    
        if (b == Float.NegativeInfinity)
          return a;
    
        if (a < b) {
          return b + StrictMath.log1p(StrictMath.exp(a - b)).toFloat
        }
        
        return a + StrictMath.log1p(StrictMath.exp(b - a)).toFloat
    }
    
    def deleteOutputFile(path: String, sc: SparkContext) = {
        val outputDir = new Path(path)
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    }
    
    def main(argv: Array[String]) {
        val args = new Conf(argv)
        val nodeCount = args.nodeCount()
        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Number of Nodes: " + nodeCount)
        log.info("Number of Iterations: " + args.iterations())
        log.info("Executors: " + args.numExecutors())
        
        val conf = new SparkConf().setAppName("PowerIterationPageRank")
        val sc = new SparkContext(conf)
        sc.setJobDescription("Takes an adjacency list and calculates PageRank for graph nodes.")
        deleteOutputFile(args.output(), sc)
        
        // Parse input adjacency list into (nodeID, Array[nodeId])
        val adjList = sc.textFile(args.input()).map(parseLine).cache()
        
        // Store PageRanks as log values, so that logarithmic arithmetic can be used to not lose precision on such small numbers.
        // Initialize ranks by setting them all to log(1/nodeCount), which is equivalent to -log(nodeCount).
        val weight = -StrictMath.log(nodeCount).toFloat
        var ranks = adjList.mapValues(v => weight)
        
        for (i <- 0 to args.iterations()) {
            /* During each PR iteration, weight gets distributed evenly from each node to its neighbours.
             * To do this, join the current page ranks with the adjacency list, then distribute weights 
             * accordingly.
             */
            log.info("ITERATION: " + i)
            
            val contributions = adjList.join(ranks).values.flatMap {
                case (neighbours, pageRank) => {
                    if (neighbours == null || neighbours.isEmpty) {
                        // Put missing mass with a key that can be extracted during the reduce phase.
                        List((-1, pageRank))
                    }
                    else {
                        // Divide a node's PageRank by the number of neighbours: ln(x) - ln(y) = ln(x/y)
                        val mass = pageRank - StrictMath.log(neighbours.size).toFloat
                        neighbours.map(neighbourId => (neighbourId, mass))
                    }
                }
            }

            // Sum the page ranks for each node
            ranks = contributions.reduceByKey {
                case (val1, val2) => {

                    if (val1 == Float.NaN || val2 == Float.NaN) {
                        log.error("***** During reduceByKey, found NaN! *****")
                    }
                    
                    (sumLogProbs(val1, val2))
                }
            }
            
            val missingMasses = ranks.lookup(-1)
            var missingMass = Float.NegativeInfinity
            if (missingMasses != null && !missingMasses.isEmpty) {
                missingMass = missingMasses(0)
                log.info("*** Missing mass was: " + missingMass + " ***")
            }
            else {
                log.warn("*** MissingMasses was null or empty. It should exist if there are any dangling nodes! ***")
            }

            // Distribute the missing mass from the dangling nodes across all nodes.
            // Also account for the random jump factor.
            val jump = (StrictMath.log(randomJump) - StrictMath.log(nodeCount)).toFloat
            ranks = ranks.map {
                case (nodeId, rank) => {
                    /* 
                     * To calculate PageRank for one node (accounting for dangling nodes with no out links),
                     * the following equation can be used:
                     *   
                     *   (1 - randomJump) * (currentPageRank + missingMass/nodeCount) + randomJump/nodeCount
                     *     
                     */
                    val link =  StrictMath.log(1.0f - randomJump).toFloat + sumLogProbs(rank, (missingMass - StrictMath.log(nodeCount)).toFloat)
                    (nodeId, sumLogProbs(jump, link))
                }
            }
        }

        ranks.mapValues { v => StrictMath.exp(v) } 
        .sortBy((nodeRank) => { nodeRank._2 }, false, 1)
        .saveAsTextFile(args.output())
    }
}