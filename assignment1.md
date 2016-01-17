Assignment 0
============

**Question 1.** _Briefly describe in prose your solution, both the pairs and stripes implementation. For example: how many MapReduce jobs? What are the input records? What are the intermediate key-value pairs? What are the final output records? A paragraph for each implementation is about the expected length._

*Pairs* - The solution I've implemented for calculating PMI using the pairs technique uses two mapreduce jobs. The first job is a simple unique word count that outputs a sequence file of unique word/count pairs that is easy to read as key/value pairs in future jobs. It also uses a custom counter to count the number of lines seen by all mappers. The second mapreduce job starts off with the same input to the mapper as the word count job. The mapper splits up the words and emits all unique pairs and a count of one as intermediate key-value pairs, which then get summed by a combiner to reduce the amount of intermediate data being shuffled. The reducer is where the PMI calculation takes place. It side-loads in the word counts from the first job to a hash map and reads in the line count to complete the PMI calculation. The final output records are word pairs with their PMI calculations (ex: "(word1, word2) 0.123").

*Stripes:* - 



**Question 2.** _What is the running time of the complete pairs implementation? What is the running time of the complete stripes implementation? (Tell me where you ran these experiments, e.g., linux.student.cs.uwaterloo.ca or your own laptop.)_

I ran these experiments on my own desktop, with these specifications:
OS: Ubuntu 14.04
RAM: 8GB DDR3 1333MHz
CPU: Intel Core i7-2600 @ 3.4GHz x 8

*Pairs* running time: 
Overall program finished in 23.513 seconds
 	Word count job finished in 4.988 seconds
 	PMI job finished in 18.51 seconds
 	
*Stripes* running time:
	**TODO**
	

**Question 3.** _Now disable all combiners. What is the running time of the complete pairs implementation now? What is the running time of the complete stripes implementation?_  
These tests were run using the same system described in question 2.

*Pairs*
Overall program finished in 31.614 seconds
	Word count job finished in 6.064 seconds
	PMI job finished in 25.536 seconds
	
 *Stripes*
 **TODO**
 
 
 **Question 4.** _(3 points) How many distinct PMI pairs did you extract?_  
I extracted **29552** distinct PMI pairs after excluding spurious pairs that co-occurred on fewer than 10 lines. There were 59104 lines of output from the PMI mapreduce job, and there were two lines output for each pair, so I divided that by two to get 29552. Without removing spurious pairs, I found 826977 distinct PMI pairs.
 
 **Question 5.** _(3 points) What's the pair (x, y) (or pairs if there are ties) with the highest PMI? Write a sentence or two to explain why such a high PMI._
 
(maine, anjou)	3.5953537413057015    
This pair has a high PMI relative to other pairs because the probability of these words co-occurring on a line is close to the probability of these words occurring at all. That means that when a line contains the word "maine", it is also likely to contain the word "anjou", and vice versa.  


 **Question 6.** _(6 points) What are the three words that have the highest PMI with "tears" and "death"? And what are the PMI values?_  
 Highest PMI with "tears":  
(tears, with)	0.3480166283180964  
(tears, are)	0.11694522732283832  
(tears, my)	-0.06588441179847718  

Highest PMI with "death":
(death, father's)	1.0990627313497934  
(death, life)	0.7381345918721788  
(death, die)	0.7293358052746564  

