package test;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment1.PairsPMI;

import tl.lin.data.pair.PairOfStrings;

public class PairsPMITest {

	private Mapper<LongWritable, Text, PairOfStrings, IntWritable>.Context _pairsContext;
	
	@Test
	public void DebugTest() throws IOException, InterruptedException {
		PairsPMI.PmiMapper pairsMapper = new PairsPMI.PmiMapper();
		LongWritable key = new LongWritable(1);
		Text value = new Text("This sentence has unique words in it.");

		pairsMapper.map(key, value, _pairsContext);
	}

}
