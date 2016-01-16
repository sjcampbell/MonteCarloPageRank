Assignment 0
============

**Question 1.** Briefly describe in prose your solution, both the pairs and stripes implementation. For example: how many MapReduce jobs? What are the input records? What are the intermediate key-value pairs? What are the final output records? A paragraph for each implementation is about the expected length. 

*Pairs* - The solution I've implemented for calculating PMI using the pairs technique uses two mapreduce jobs. The first job is a simple unique word count that outputs a sequence file of unique word/count pairs that is easy to read as key/value pairs in future jobs. It also uses a custom counter to count the number of lines seen by all mappers. The second mapreduce job starts off with the same input to the mapper as the word count. The mapper splits up the words and emits all unique pairs and a count of one as intermediate key-value pairs, which then get summed by a combiner to reduce the amount of intermediate data being shuffled. The reducer is where the PMI calculation takes place. It side-loads in the word counts from the first job and reads in the line count to complete the PMI calculation. The final output records are word combinations and PMI calculations for the word combinations (ex: "(word1, word2) 0.123").

Stripes:



**Question 2.** What is the running time of the complete pairs implementation? What is the running time of the complete stripes implementation? (Tell me where you ran these experiments, e.g., linux.student.cs.uwaterloo.ca or your own laptop.)

I ran these experiments on my own desktop, with these specifications:
OS: Ubuntu 14.04
RAM: 8GB DDR3 1333MHz
CPU: Intel Core i7-2600 @ 3.4GHz x 8

*Pairs* running time: 
Overall program finished in 27.617 seconds
 	Word count job finished in 5.972 seconds
 	Pointwise mutual information job finished in 21.628 seconds
 	
*Stripes* running time:
	**TODO**
 