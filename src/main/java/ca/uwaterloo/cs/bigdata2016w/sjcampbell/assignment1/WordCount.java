package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment1;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.google.common.collect.Sets;

/*
 * This mapreduce job generates a count of words by summing unique words from each line.
 * It also counts the number of lines in a "PmiLineCount" counter
 */
public class WordCount {

	public static final String LineCountProperty = "PmiLineCount";
	
	public static enum Count {
		LINES
	}
	
	public static void ConfigureJob(Job job) {
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(WordCount.WordCountMapper.class);
		job.setCombinerClass(WordCount.WordCountReducer.class);
		job.setReducerClass(WordCount.WordCountReducer.class);
	}
	
	/*
	 * Mapper and Reducer to calculate word count in the input
	 */
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private static final Text WORD = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			context.getCounter(Count.LINES).increment(1);
			String line = ((Text)value).toString();
			StringTokenizer itr = new StringTokenizer(line);
			Set<String> set = Sets.newHashSet();
			
			while(itr.hasMoreTokens()) {
				String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
				if (w.length() == 0) continue;
				set.add(w);
			}
			
			if (set.size() == 0) return;
			
			for(String s : set) {
				WORD.set(s);
				context.write(WORD, ONE);
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final static IntWritable SUM = new IntWritable();

	    @Override
	    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	        throws IOException, InterruptedException {
	      // Sum up values.
	      Iterator<IntWritable> iter = values.iterator();
	      int sum = 0;
	      while (iter.hasNext()) {
	        sum += iter.next().get();
	      }
	      SUM.set(sum);
	      context.write(key, SUM);
	    }
	}
}
