package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment1;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.ParserProperties;

import com.google.common.collect.Sets;

import tl.lin.data.pair.PairOfStrings;

/**
 * Calculating Pointwise Mutual Information
 */
public class PairsPMI extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(PairsPMI.class);
	
	public static class PmiMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
		// Reuse objects to save overhead of object creation.
		private final static IntWritable ONE = new IntWritable(1);
		private final static PairOfStrings PAIR = new PairOfStrings();
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = ((Text) value).toString();
			StringTokenizer itr = new StringTokenizer(line);

			int cnt = 0;
			Set<String> set = Sets.newHashSet();
			while (itr.hasMoreTokens()) {
				cnt++;
				String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
				if (w.length() == 0) continue;
				set.add(w);
				if (cnt >= 100) break;
			}

			if (set.size() < 2) return;

			String[] words = new String[set.size()];
			words = set.toArray(words);

			for (int i = 0; i < set.size() - 1; i++)
			{
				for (int j = 0; j < set.size() - 1; j++)
				{
					if (i == j) continue;
					
					PAIR.set(words[i], words[j]);
					context.write(PAIR, ONE);
				}
			}
		}
	}
	
	protected static class PmiCombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
		private final static IntWritable SUM = new IntWritable();
		
		@Override
		public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
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

	// Reducer: sums up all the counts.
	protected static class PmiReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, DoubleWritable> {
		private final static DoubleWritable PMI = new DoubleWritable();
		private static HashMap<String, Integer> wordCounts = new HashMap<>();
		private static int lineCount;
		
		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			
			lineCount = conf.getInt(PmiConfiguration.LineCountProperty, -1);
			
			URI[] fileUris = context.getCacheFiles();
			for(URI fileUri : fileUris) {
				System.out.println("Found cached file: " + fileUri.toString());
				
				if (fileUri.toString().contains("part-r-")) {
					SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(new Path(fileUri)));
					try
					{
						Text key = new Text();
					    IntWritable value = new IntWritable();
					
					    int count = 0;
					    
					    while(reader.next(key, value)) {
					    	count++;
					    	wordCounts.put(key.toString(), value.get());
					    }
					    
					    System.out.println("Read in " + count + " key/values from cached file.");
					    System.out.println("Last key: " + key + ", last value: " + value);
					}
					catch (Exception ex) {
						System.err.println("Error: Failed while reading sequence file in PMI reducer setup: " + fileUri.toString());
				    	throw ex;
					}
					finally{
						reader.close();
					}
				}
			}
		}
		
		@Override
		public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// Sum up values.
			Iterator<IntWritable> iter = values.iterator();
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}
			
			if (sum < 10) {
				// Ignore pairs that appear on less than 10 lines.
				return;
			}
			
			// Calculate  N*c(x,y)/(c(x)*c(y))
			// c()	: Count function 
			// N	: Total number of lines
			String wordx = key.getKey();
			String wordy = key.getValue();
			double cx = wordCounts.get(wordx);
			double cy = wordCounts.get(wordy);
			
			if (cx == 0) {
				LOG.error("Word count was 0 for c(x), x: " + key.getKey());
				return;
			}
			else if (cy == 0) {
				LOG.error("Word count was 0 for c(y), y: " + key.getValue());
				return;
			}
			
			double pmi = Math.log10((double)lineCount * sum / (cx * cy));
			PMI.set(pmi);
			context.write(key, PMI);
		}
	}
	
	/**
	 * Creates an instance of this tool.
	 */
	public PairsPMI() {}

	/**
	 * Runs this tool.
	 */
	public int run(String[] argv) throws Exception {
		Args args = new Args();
		CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

		try {
			parser.parseArgument(argv);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return -1;
		}

		// Set up and run word count job
		Configuration conf = getConf();
		PmiConfiguration pmiConfig = new PmiConfiguration(PairsPMI.class, conf, args);

		Path intermediate = new Path(args.output + "_int");
		Job jobWC = pmiConfig.SetupWordCountJob(intermediate);
		long totalStartTime = System.currentTimeMillis();
		long lineCount = pmiConfig.RunWordCountJob(jobWC);
		
		conf.setLong(PmiConfiguration.LineCountProperty, lineCount);
		System.out.println("Line counter result: " + lineCount);
		
		// Job 2 - PMI Calculation
		Job jobPmi = Job.getInstance(conf);
		jobPmi.setJobName(PairsPMI.class.getSimpleName() + "_CalculatePMI");
		jobPmi.setJarByClass(PairsPMI.class);
		jobPmi.setNumReduceTasks(args.numReducers);

		Path outputDir = new Path(args.output);
		FileInputFormat.setInputPaths(jobPmi, new Path(args.input));
		FileOutputFormat.setOutputPath(jobPmi, new Path(args.output));
		
		configureJobPmiParameters(jobPmi);
		pmiConfig.AddJobOutputToCache(conf, jobPmi, intermediate);
		
		// Delete the output directory if it exists
		FileSystem.get(conf).delete(outputDir, true);
		
		long startTime2 = System.currentTimeMillis();
		jobPmi.waitForCompletion(true);
		LOG.info("Pointwise mutual information job finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");
		LOG.info("Overall Job Finished in " + (System.currentTimeMillis() - totalStartTime) / 1000.0 + " seconds");
		
		return 0;
	}
	
	private void configureJobPmiParameters(Job jobPmi) {
		jobPmi.setMapOutputKeyClass(PairOfStrings.class);
		jobPmi.setMapOutputValueClass(IntWritable.class);
		jobPmi.setOutputKeyClass(PairOfStrings.class);
		jobPmi.setOutputValueClass(DoubleWritable.class);
		jobPmi.setOutputFormatClass(TextOutputFormat.class);
		jobPmi.setMapperClass(PmiMapper.class);
		jobPmi.setReducerClass(PmiReducer.class);
		jobPmi.setCombinerClass(PmiCombiner.class);
		
		jobPmi.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
		jobPmi.getConfiguration().set("mapreduce.map.memory.mb", "3072");
		jobPmi.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
		jobPmi.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
		jobPmi.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");
	}
	
	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PairsPMI(), args);
	}
}
