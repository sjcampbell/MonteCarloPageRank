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
import org.apache.hadoop.io.LongWritable;
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

import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.MapKI.Entry;
import tl.lin.data.pair.PairOfStrings;

public class StripesPMI extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(StripesPMI.class);
	
	private static class PmiMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
		private final static Text KEY = new Text();
		
		private final static HMapStIW MAP = new HMapStIW();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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

			// For each word on the line emit it as the key with the rest of the words in a map.
			for (int i = 0; i < set.size() - 1; i++)
			{
				MAP.clear();
				
				for (int j = 0; j < set.size() - 1; j++)
				{
					if (i == j) continue;
					MAP.increment(words[j]);
				}
				
				KEY.set(words[i]);
				context.write(KEY, MAP);
			}
		}
	}
	
	private static class PmiCombiner extends Reducer<Text, HMapStIW, Text, HMapStIW> {
		@Override
		public void reduce(Text key, Iterable<HMapStIW> values, Context context) throws IOException, InterruptedException {
			Iterator<HMapStIW> iter = values.iterator();
			HMapStIW map = new HMapStIW();
			
			while(iter.hasNext()) {
				map.plus(iter.next());
			}
			
			context.write(key, map);
		}
	}
	
	protected static class PmiReducer extends Reducer<Text, HMapStIW, PairOfStrings, DoubleWritable> {
		private final static DoubleWritable PMI = new DoubleWritable();
		private final static PairOfStrings PAIR = new PairOfStrings();
		private static HashMap<String, Integer> wordCounts = new HashMap<>();
		private static int lineCount;
		
		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			lineCount = conf.getInt(PmiConfiguration.LineCountProperty, -1);
			URI[] fileUris = context.getCacheFiles();
			PmiConfiguration pmiConfig = new PmiConfiguration(StripesPMI.class, conf);
			wordCounts = pmiConfig.GetDistributedCacheMap(fileUris); 
		}
		
		@Override
		public void reduce(Text key, Iterable<HMapStIW> values, Context context)
				throws IOException, InterruptedException {
			Iterator<HMapStIW> iter = values.iterator();
			HMapStIW map = new HMapStIW();
			
			while(iter.hasNext()) {
				map.plus(iter.next());
			}
			
			// Now we have a key and a complete map of word pairs and counts for that key.
			// Write the word pairs one at a time, calculating the PMI for each pair.
			Entry<String>[] mapEntries = map.getEntriesSortedByKey();
			if (mapEntries == null) {
				LOG.error("There were no map entries for key: " + key);
				return;
			}
			
			for(Entry<String> entry : map.getEntriesSortedByKey()) {
				if (entry.getValue() < 10) {
					// Skip any word pairs that appear on fewer than 10 lines.
					continue;
				}
				
				// Calculate  N*c(x,y)/(c(x)*c(y))
				// c()	: Count function 
				// N	: Total number of lines
				String wordx = key.toString();
				String wordy = entry.getKey();
				double cx = wordCounts.get(wordx);
				double cy = wordCounts.get(wordy);
				
				if (cx == 0) {
					LOG.error("Word count was 0 for c(x), x: " + wordx);
					return;
				}
				else if (cy == 0) {
					LOG.error("Word count was 0 for c(y), y: " + wordy);
					return;
				}
				
				double pmi = Math.log10((double)lineCount * entry.getValue() / (cx * cy));
				PMI.set(pmi);
				PAIR.set(wordx, wordy);
				context.write(PAIR, PMI);
			}
		}
	}
	
	/**
	 * Creates an instance of this tool.
	 */
	public StripesPMI() {}
	
	/*
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
		PmiConfiguration pmiConfig = new PmiConfiguration(StripesPMI.class, conf);

		Path intermediate = new Path(args.output + "_int");
		Job jobWC = pmiConfig.SetupWordCountJob(intermediate, args);
		long totalStartTime = System.currentTimeMillis();
		long lineCount = pmiConfig.RunWordCountJob(jobWC);
		
		conf.setLong(PmiConfiguration.LineCountProperty, lineCount);
		System.out.println("Line counter result: " + lineCount);
		
		// Job 2 - PMI Calculation
		Job jobPmi = Job.getInstance(conf);
		jobPmi.setJobName(StripesPMI.class.getSimpleName() + "_CalculatePMI");
		jobPmi.setJarByClass(StripesPMI.class);
		jobPmi.setNumReduceTasks(args.numReducers);

		Path outputDir = new Path(args.output);
		FileInputFormat.setInputPaths(jobPmi, new Path(args.input));
		FileOutputFormat.setOutputPath(jobPmi, new Path(args.output));
		
		configureJobPmiParameters(jobPmi);
		pmiConfig.AddFilesInPathToJobCache(conf, jobPmi, intermediate);
		
		// Delete the output directory if it exists
		FileSystem.get(conf).delete(outputDir, true);
		
		long startTime2 = System.currentTimeMillis();
		jobPmi.waitForCompletion(true);
		LOG.info("Pointwise mutual information job finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");
		LOG.info("Overall Job Finished in " + (System.currentTimeMillis() - totalStartTime) / 1000.0 + " seconds");
		
		return 0;
	}
	
	private void configureJobPmiParameters(Job jobPmi) {
		jobPmi.setMapOutputKeyClass(Text.class);
		jobPmi.setMapOutputValueClass(HMapStIW.class);
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
		ToolRunner.run(new StripesPMI(), args);
	}
}
