# Monte Carlo Page Rank in Spark

#### For project details, see the project paper [here](Project%20Paper/McPageRankSpark.pdf)  


#### About this project
The PageRank algorithm has been widely used and re-searched since its introduction in 1998, due in part to its usefulness for ranking web pages at Google. Its purpose is to assign “importance” to nodes in a directed graph based on the graph’s internal link structure. In the context of web pages, it assigns an importance score, or “PageRank” to pages.

The original implementation of PageRank calculated it using a method called "Power Iteration", but this project calculates it using a Monte Carlo method based on research by K. Avrachenkov et al.

### How to run it
1. Clone the repo (master branch): sjcampbell/MonteCarloPageRank
2. Build the source: ``mvn package``
3. To run the Power Iteration Algorithm:  
``spark-submit --class com.sjcampbell.mcpagerank.PowerIterationPageRank target/mcpagerank-0.1.0-SNAPSHOT.jar --input web-stanford-adjacency.txt --iterations 1 --node-count 281731``
4. To run the Monte Carlo Algorithm:  
``spark-submit --class com.sjcampbell.mcpagerank.MonteCarloPageRank target/mcpagerank-0.1.0-SNAPSHOT.jar --input web-stanford-adjacency.txt --iterations 1 --node-count 6301 --walks 1``

### Need some data?
Here's a good start:   https://snap.stanford.edu/data/web-Stanford.html  
You'll need to format it from pairs into an adjacency list. I've put together a spark job for that, and after building (above), it can be run in spark:  
``spark-submit --class com.sjcampbell.mcpagerank.ConvertPairsToAdjacencyList target/mcpagerank-0.1.0-SNAPSHOT.jar --input web-Stanford.txt``
