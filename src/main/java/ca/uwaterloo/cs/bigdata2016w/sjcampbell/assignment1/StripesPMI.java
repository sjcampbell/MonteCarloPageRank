package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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

import tl.lin.data.pair.PairOfStrings;

public class StripesPMI extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(StripesPMI.class);
	
	private static class PmiMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
		//private final static IntWritable ONE = new IntWritable(1);
		//private final static PairOfStrings PAIR = new PairOfStrings();
		
		@Override
		public void map(LongWritable key, Text value, Context context) {
			// TODO: Stripes Mapper
		}
	}
	
	private static class PmiCombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
		
		@Override
		public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context) {
			// TODO: Stripes Combiner
		}
	}
	
	protected static class PmiReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, DoubleWritable> {
		@Override
		protected void setup(Context context) throws IOException {
			// TODO: Stripes reducer
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
		PmiConfiguration pmiConfig = new PmiConfiguration(StripesPMI.class, conf, args);

		Path intermediate = new Path(args.output + "_int");
		Job jobWC = pmiConfig.SetupWordCountJob(intermediate);
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
		ToolRunner.run(new StripesPMI(), args);
	}
}
