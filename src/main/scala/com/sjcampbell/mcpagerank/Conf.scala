package com.sjcampbell.mcpagerank

import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, numExecutors)
  val input = opt[String](descr = "input path", required = true)
  val nodeCount = opt[Int](descr = "number of nodes in input graph", required = true)
  val iterations = opt[Int](descr = "number of iterations", required = false, default = Some(10))
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
}