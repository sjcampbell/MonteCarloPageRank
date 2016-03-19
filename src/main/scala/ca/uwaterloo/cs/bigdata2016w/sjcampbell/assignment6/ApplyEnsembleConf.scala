package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment6

import org.rogach.scallop._

class ApplyEnsembleConf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output", required = true)
    val model = opt[String](descr = "model", required = true)
    val method = opt[String](descr = "method", required = true)
}