package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment4;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListOfIntsWritable;

/**
 * <p>
 * Driver program that takes a plain-text encoding of a directed graph and builds corresponding
 * Hadoop structures for representing the graph.
 * </p>
 */
public class BuildPersonalizedPageRankRecords extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildPersonalizedPageRankRecords.class);

  private static final String NODE_CNT_FIELD = "node.cnt";

  private static class MyMapper extends Mapper<LongWritable, Text, IntWritable, PageRankNode> {
    private static final IntWritable nid = new IntWritable();
    private static final PageRankNode node = new PageRankNode();
    private static int[] sources;
    private static float[] sourcePageRanks;
    
    @Override
    public void setup(Mapper<LongWritable, Text, IntWritable, PageRankNode>.Context context) {
		sources = context.getConfiguration().getInts("Sources");
		if (sources == null || sources.length == 0)
		{
			throw new RuntimeException("Sources configuration was not found in the personalized PageRank records builder mapper.");
		}
    	
    	int n = context.getConfiguration().getInt(NODE_CNT_FIELD, 0);
    	if (n == 0) {
    		throw new RuntimeException(NODE_CNT_FIELD + " cannot be 0!");
    	}
    	node.setType(PageRankNode.Type.Complete);
    	
    	// SSPR: All nodes except the source node(s) get a 0 ranking to start.
    	sourcePageRanks = new float[sources.length];
    	//node.setPageRankValues(Float.NEGATIVE_INFINITY);	
    }

    @Override
    public void map(LongWritable key, Text t, Context context) throws IOException, InterruptedException {
    	String[] arr = t.toString().trim().split("\\s+");

    	nid.set(Integer.parseInt(arr[0]));
    	node.setNodeId(Integer.parseInt(arr[0]));
    	
    	// Set page ranks to 1 only in source nodes. 
    	for (int i = 0; i < sources.length; i++) {
    		if (nid.equals(new IntWritable(sources[i]))) {
    			sourcePageRanks[i] = 0.0f; // 0.0f == StrictMath.log(1.0f)
        	}	
    		else {
    			sourcePageRanks[i] = Float.NEGATIVE_INFINITY;
    		}
    	}
    	node.setPageRanks(sourcePageRanks);

    	if (arr.length == 1) {
    		node.setAdjacencyList(new ArrayListOfIntsWritable());
    	} 
    	else {
    		int[] neighbors = new int[arr.length - 1];
    		for (int i = 1; i < arr.length; i++) {
    			neighbors[i - 1] = Integer.parseInt(arr[i]);
    		}

    		node.setAdjacencyList(new ArrayListOfIntsWritable(neighbors));
    	}

    	context.getCounter("graph", "numNodes").increment(1);
    	context.getCounter("graph", "numEdges").increment(arr.length - 1);

    	if (arr.length > 1) {
    		context.getCounter("graph", "numActiveNodes").increment(1);
    	}

    	context.write(nid, node);
    }
  }

  public BuildPersonalizedPageRankRecords() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_NODES = "numNodes";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes").create(NUM_NODES));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("sources").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(NUM_NODES) || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
    String sources = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: " + BuildPersonalizedPageRankRecords.class.getSimpleName());
    LOG.info(" - inputDir: " + inputPath);
    LOG.info(" - outputDir: " + outputPath);
    LOG.info(" - numNodes: " + n);
    LOG.info(" - sources: " + n);

    Configuration conf = getConf();
    conf.setInt(NODE_CNT_FIELD, n);
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

    Job job = Job.getInstance(conf);
    job.setJobName(BuildPersonalizedPageRankRecords.class.getSimpleName() + ":" + inputPath);
    job.setJarByClass(BuildPersonalizedPageRankRecords.class);

    job.setNumReduceTasks(0);
    job.getConfiguration().set("Sources", sources);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MyMapper.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildPersonalizedPageRankRecords(), args);
  }
}
