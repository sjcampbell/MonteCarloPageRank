package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment1;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import com.google.common.collect.Sets;

import tl.lin.data.pair.PairOfStrings;

/**
 * Simple word count demo.
 */
public class PairsPMI extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(PairsPMI.class);

	public static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
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
				for (int j = i + 1; j < set.size(); j++)
				{
					// TODO: Is it necessary to only include unique combinations of words on a line?
					if (words[i].compareTo(words[j]) < 0)
					{
						PAIR.set(words[i], words[j]);
						context.write(PAIR, ONE);

						PAIR.set(words[i], "*");
						context.write(PAIR, ONE);
					}
					else
					{
						PAIR.set(words[j], words[i]);
						context.write(PAIR, ONE);
						
						PAIR.set(words[j], "*");
						context.write(PAIR, ONE);
					}
				}
			}
		}
	}

	// Reducer: sums up all the counts.
	protected static class MyReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
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

		LOG.info("Tool: " + PairsPMI.class.getSimpleName());
		LOG.info(" - input path: " + args.input);
		LOG.info(" - output path: " + args.output);
		LOG.info(" - number of reducers: " + args.numReducers);

		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName(PairsPMI.class.getSimpleName());
		job.setJarByClass(PairsPMI.class);

		job.setNumReduceTasks(args.numReducers);

		FileInputFormat.setInputPaths(job, new Path(args.input));
		FileOutputFormat.setOutputPath(job, new Path(args.output));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		// Delete the output directory if it exists already.
		Path outputDir = new Path(args.output);
		FileSystem.get(conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PairsPMI(), args);
	}
}
