Assignment 1
============

**Question 1.** _Briefly describe in prose your solution, both the pairs and stripes implementation. For example: how many MapReduce jobs? What are the input records? What are the intermediate key-value pairs? What are the final output records? A paragraph for each implementation is about the expected length._

*Pairs* - The solution I've implemented for calculating PMI using the pairs technique uses two mapreduce jobs. The first job is a simple unique word count that outputs a sequence file of unique word/count pairs that is easy to read as key/value pairs in future jobs. It also uses a custom counter to count the number of lines seen by all mappers. The second mapreduce job starts off with the same input to the mapper as the word count job. The mapper splits up the words and emits all unique pairs and a count of one as intermediate key-value pairs, which then get summed by a combiner to reduce the amount of intermediate data being shuffled. The reducer is where the PMI calculation takes place. It side-loads in the word counts from the first job to a hash map and reads in the line count to complete the PMI calculation. The final output records are word pairs with their PMI calculations (ex: "(word1, word2) 0.123").

*Stripes:* - The stripes solution also uses two mapreduce jobs. The first is exactly the same as the one used in the pairs approach to calculate word counts and the total number of lines. The second mapreduce job is used to calculate PMI. The mapper receives a line of text and outputs a word as a key, and a map as a value, where the map contains all the other unique words found in the input line. Combiners are used to sum up the maps for a word using a vector-like addition. The reducer is given a word with its corresponding map. It calculates PMI values for all the pairs made from combining the word with the map keys. Then it outputs each pair individually with the corresponding PMI calculation.

**Question 2.** _What is the running time of the complete pairs implementation? What is the running time of the complete stripes implementation? (Tell me where you ran these experiments, e.g., linux.student.cs.uwaterloo.ca or your own laptop.)_

I ran these experiments on my own desktop, with these specifications:
OS: Ubuntu 14.04
RAM: 8GB DDR3 1333MHz
CPU: Intel Core i7-2600 @ 3.4GHz x 8

*Pairs* running time (5 reducers):  
Overall program finished in 28.012 seconds  
 	Word count job finished in 5.465 seconds  
 	PMI job finished in 22.531 seconds   	
 	
*Stripes* running time (5 reducers):  
Overall program finished in 15.43 seconds  
	Word count job finished in 5.429 seconds  
	PMI job finished in 9.98 seconds  
	

**Question 3.** _Now disable all combiners. What is the running time of the complete pairs implementation now? What is the running time of the complete stripes implementation?_  
These tests were run using the same system described in question 2.

*Pairs*:  
Overall program finished in 33.185 seconds  
	Word count job finished in 6.649 seconds  
	PMI job finished in 26.521 seconds  
	
*Stripes*:  
Overall program finished in 16.708 seconds  
	Word count job finished in 6.094 seconds  
	PMI job finished in 10.525 seconds  
 
 
 **Question 4.** _(3 points) How many distinct PMI pairs did you extract?_  
I extracted **38599** distinct PMI pairs after excluding spurious pairs that co-occurred on fewer than 10 lines. There were 77198 lines of output from the PMI mapreduce job, and there were two lines output for each pair, so I divided that by two to get 38599.
 
 **Question 5.** _(3 points) What's the pair (x, y) (or pairs if there are ties) with the highest PMI? Write a sentence or two to explain why such a high PMI._  
(maine, anjou)	3.6331423021951013  
This pair has a high PMI relative to other pairs because the probability of these words co-occurring on a line is close to the probability of these words occurring at all. That means that when a line contains the word "maine", it is also likely to contain the word "anjou", and vice versa.  

 **Question 6.** _(6 points) What are the three words that have the highest PMI with "tears" and "death"? And what are the PMI values?_  
 Highest PMI with "tears":  
(tears, shed)	2.111790076876236  
(tears, salt)	2.0528122169168985  
(tears, eyes)	1.1651669643071034  

Highest PMI with "death":  
(death, father's)	1.1202520304197314  
(death, die)	0.7541593889996885  
(death, life)	0.7381345918721788  

**Question 7.** _(6 points) In the Wikipedia sample, what are the three words that have the highest PMI with "waterloo" and "toronto"? And what are the PMI values?_  
Three largets PMI values with "toronto":  
(toronto, mimico)       1.6847684214104595
(toronto, marlboros)    1.6122177542618477
(toronto, argonauts)    1.5844654655050636
  
Three largest PMI values with "waterloo":  
(waterloo, kitchener)   1.6725974629805342  
(waterloo, napoleonic)  0.7998774154037366  
(waterloo, napoleon)    0.7889055071141662  



