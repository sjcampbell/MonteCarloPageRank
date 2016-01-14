package test;

import static org.junit.Assert.*;

import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

import ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment1.PairsPMI;

import tl.lin.data.pair.PairOfStrings;

public class PairsPMITest {

	private Mapper<LongWritable, Text, PairOfStrings, IntWritable>.Context _pairsContext;
	
	@Test
	public void DebugTest() throws IOException, InterruptedException {
		// Arrange
		_pairsContext = mock(Context.class);
		Configuration conf = mock(Configuration.class);
		PairsPMI.MyMapper pairsMapper = new PairsPMI.MyMapper();
		
		LongWritable key = new LongWritable(1);
		Text value = new Text("This sentence has unique words in it.");
		
		// Act
		pairsMapper.map(key, value, _pairsContext);
	}

}
