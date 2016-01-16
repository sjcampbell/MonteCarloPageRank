package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import com.google.common.collect.Sets;

import tl.lin.data.pair.PairOfStrings;

/**
 * Simple word count demo.
 */
public class PairsPMI extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(PairsPMI.class);

	private enum Count {
		LINES
	}
	
	public static class PmiMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
		// Reuse objects to save overhead of object creation.
		private final static IntWritable ONE = new IntWritable(1);
		private final static PairOfStrings PAIR = new PairOfStrings();
		private static HashMap<Text, IntWritable> wordCounts = new HashMap<>();
		
		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
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
					    	wordCounts.put(key, value);
					    }
					    
					    System.out.println("Read in " + count + " key/values from cached file.");
					}
					catch (Exception ex) {
						System.err.println("Error: Failed to read first key from sequence file: " + fileUri.toString());
				    	throw ex;
					}
					finally{
						reader.close();
					}
				}
				
				/* TODO: This would be useful with a text type output.
				 * if (fileUri.toString().endsWith(".dat")) {
					
					// TODO: Read file and fill in wordCounts;
					File cacheFile = new File(fileUri);
					FileInputStream fis = new FileInputStream(cacheFile);
					BufferedReader br = new BufferedReader(new InputStreamReader(fis));
					
					String line = null;
					while ((line=br.readLine()) != null) {
						// Parse line and put it into the HashMap
					}*/
			}
		}
		
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
				for (int j = 0; j < set.size(); j++)
				{
					if (i == j) continue;
					
					PAIR.set(words[i], words[j]);
					context.write(PAIR, ONE);
				
					PAIR.set(words[i], "*");
					context.write(PAIR, ONE);
				}
			}
		}
	}

	// Reducer: sums up all the counts.
	protected static class PmiReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
		// Reuse objects.
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
	
	protected static class PmiPartitioner extends Partitioner<PairOfStrings, IntWritable> {
	    @Override
	    public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
	      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	    }
	  }

	/*
	 * Mapper and Reducer to calculate relative frequencies of all words in the input.
	 */
	private static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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

	private static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
	
	/**
	 * Creates an instance of this tool.
	 */
	public PairsPMI() {}

	public static class Args {
		@Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
		public String input;

		@Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
		public String output;

		@Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
		public int numReducers = 1;

		@Option(name = "-imc", usage = "use in-mapper combining")
		boolean imc = false;
	}

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

		logArguments(args);

		Configuration conf = getConf();
		
		// Job 1 - Word count and total line count
		Job jobWC = Job.getInstance(conf);
		jobWC.setJobName(PairsPMI.class.getSimpleName() + "_WordCount");
		jobWC.setJarByClass(PairsPMI.class);
		jobWC.setNumReduceTasks(args.numReducers);
		Path intermediate = new Path(args.output + "_int");
		FileInputFormat.setInputPaths(jobWC, new Path(args.input));
		FileOutputFormat.setOutputPath(jobWC, intermediate);
		configureJobWcTypes(jobWC);
		
		// Delete the intermediate directory if it exists already.
		FileSystem.get(conf).delete(intermediate, true);
		
		long startTime = System.currentTimeMillis();
		jobWC.waitForCompletion(true);
		LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		long lineCount = jobWC.getCounters().findCounter(Count.LINES).getValue();

		conf.setLong("PmiLineCount", lineCount);
		System.out.println("Line counter result: " + lineCount);
		
		// Job 2 - PMI Calculation
		Job jobPmi = Job.getInstance(conf);
		jobPmi.setJobName(PairsPMI.class.getSimpleName() + "_CalculatePMI");
		jobPmi.setJarByClass(PairsPMI.class);
		jobPmi.setNumReduceTasks(args.numReducers);

		Path outputDir = new Path(args.output);
		FileInputFormat.setInputPaths(jobPmi, new Path(args.input));
		FileOutputFormat.setOutputPath(jobPmi, new Path(args.output));
		
		// TODO: Change combiner all up in here.
		configureJobPmiTypes(jobPmi);
		//addSingleFileToCache(conf, jobPmi, intermediate);
		addJobOutputToCache(conf, jobPmi, intermediate);
		
		// Delete the output directory if it exists
		FileSystem.get(conf).delete(outputDir, true);
		
		long startTime2 = System.currentTimeMillis();
		jobPmi.waitForCompletion(true);
		LOG.info("Pointwise mutual information job finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");
		
		return 0;
	}

	private void logArguments(Args args) {
		LOG.info("Tool: " + PairsPMI.class.getSimpleName());
		LOG.info(" - input path: " + args.input);
		LOG.info(" - output path: " + args.output);
		LOG.info(" - number of reducers: " + args.numReducers);
	}

	// Thanks to @foxroot
	// http://stackoverflow.com/a/30230251/2565692
	private void addJobOutputToCache(Configuration config, Job job, Path filePath) throws IOException {
		FileSystem fs = FileSystem.get(config);
		FileStatus[] fileList = fs.listStatus(filePath, 
                new PathFilter(){
                      @Override public boolean accept(Path path){
                             return path.getName().startsWith("part-");
                      } 
                 } );
	
		for(int i=0; i < fileList.length;i++){
			job.addCacheFile(fileList[i].getPath().toUri());
		}
	}

	private void addSingleFileToCache(Configuration config, Job job, Path filePath) throws IOException {
		FileSystem fs = FileSystem.get(config);
		Path dstFile = new Path(filePath + ".dat");
		
		// Merge and cache output file.
		FileUtil.copyMerge(fs, filePath, fs, dstFile, false, config, null);
		job.addCacheFile(dstFile.toUri());
	}
	
	private void configureJobWcTypes(Job jobRF) {
		jobRF.setMapOutputKeyClass(Text.class);
		jobRF.setMapOutputValueClass(IntWritable.class);
		jobRF.setOutputKeyClass(Text.class);
		jobRF.setOutputValueClass(IntWritable.class);
		jobRF.setOutputFormatClass(SequenceFileOutputFormat.class);
		jobRF.setMapperClass(WordCountMapper.class);
		jobRF.setCombinerClass(WordCountReducer.class);
		jobRF.setReducerClass(WordCountReducer.class);
	}
	
	private void configureJobPmiTypes(Job jobPmi) {
		jobPmi.setMapOutputKeyClass(PairOfStrings.class);
		jobPmi.setMapOutputValueClass(IntWritable.class);
		jobPmi.setOutputKeyClass(PairOfStrings.class);
		jobPmi.setOutputValueClass(IntWritable.class);
		jobPmi.setOutputFormatClass(TextOutputFormat.class);
		jobPmi.setMapperClass(PmiMapper.class);
		jobPmi.setReducerClass(PmiReducer.class);
		
		// TODO: Change the combiner.
		// jobPmi.setCombinerClass(PmiReducer.class);
	}
	
	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PairsPMI(), args);
	}
}
