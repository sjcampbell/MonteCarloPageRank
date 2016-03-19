package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment6

class LineParser {
    def parseDataLine(line: String) : (String, String, Array[Int]) = {
        val split = line.split(" ")
        val docid = split(0)
        val isSpam = split(1)
        
        val features = split.drop(2).map(f => f.toInt)
        
        (docid, isSpam, features)
    }
    
    def parseModelLine(line: String) : (Int, Double) = {
        val split = line.split("[\\(,\\)]")

        // feature, weight 
        (split(1).toInt, split(2).toDouble)
    }
}