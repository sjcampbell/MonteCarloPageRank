package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment6

class SpamClassifier {
    def spamminess(features: Array[Int], weights: scala.collection.Map[Int, Double]) : Double = {
        var score = 0d
        features.foreach(f => if (weights.contains(f)) score += weights(f))
        score
    }
    
    def EnsembleAverage(sp0: Double, sp1: Double, sp2: Double) : (Double, String) = {
        val avg = (sp0 + sp1 + sp2) / 3.0
        if (avg > 0) (avg, "spam") else (avg, "ham")
    }
    
    def EnsembleVote(sp0: Double, sp1: Double, sp2: Double) : (Int, String) = {
        var score = 0
        if (sp0 > 0) score = score + 1 else score = score - 1
        if (sp1 > 0) score = score + 1 else score = score - 1
        if (sp2 > 0) score = score + 1 else score = score - 1
        if (score > 0) (score, "spam") else (score, "ham") 
    }
}