package com.sjcampbell.mcpagerank

import org.rogach.scallop._

class BuildAdjListConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, numExecutors)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
}