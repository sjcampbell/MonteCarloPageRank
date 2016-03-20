package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment6

import org.rogach.scallop._

class TrainConf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model", required = true)
  val shuffle = toggle("shuffle")
}