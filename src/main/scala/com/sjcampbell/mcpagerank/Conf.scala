package com.sjcampbell.mcpagerank

import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, numExecutors)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val nodeCount = opt[Int](descr = "number of nodes in input graph", required = true)
  val iterations = opt[Int](descr = "number of iterations", required = false, default = Some(10))
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
}

class McConf(args: Seq[String]) extends Conf(args) {
    val walks = opt[Int](descr = "number of random walks PER GRAPH NODE to execute", required = false, default = Some(10000))
}