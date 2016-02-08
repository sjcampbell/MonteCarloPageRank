package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static class MyMapper extends
      Mapper<IntWritable, PageRankNode, PairOfInts, FloatWritable> {
    
	  private static int[] sources;
	  private ArrayList<TopScoredObjects<Integer>> sourceQueues;

    @Override
    public void setup(Context context) throws IOException {
    	int k = context.getConfiguration().getInt("n", 100);
    	sources = getSources(context);

    	LOG.info("!! Mapper, sources length: " + sources.length);
    	
    	sourceQueues = new ArrayList<TopScoredObjects<Integer>>(sources.length);
    	for (int i = 0; i < sources.length; i++) {
    		sourceQueues.add(new TopScoredObjects<Integer>(k));
    	}
    	
    	LOG.info("!! Mapper setup complete.");
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {
    	float[] pageRanks = node.getPageRanks();
    	for(int i = 0; i < sources.length; i++) {
    		sourceQueues.get(i).add(node.getNodeId(), pageRanks[i]);
    	}
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    	PairOfInts key = new PairOfInts();
    	FloatWritable value = new FloatWritable();
    
    	// For each source, write emit key: <SourceID, NodeID>, value: PageRank 
    	for(int i = 0; i < sources.length; i++) {
    		for (PairOfObjectFloat<Integer> pair : sourceQueues.get(i).extractAll()) {
    			key.set(sources[i], pair.getLeftElement());
    			value.set(pair.getRightElement());
    			
    			context.write(key, value);
    		}
    	}
	}
  }

  private static class MyReducer extends
      Reducer<PairOfInts, FloatWritable, PairOfInts, FloatWritable> {
    private static ArrayList<TopScoredObjects<Integer>> sourceQueues;
    private int[] sources;
    
    @Override
    public void setup(Context context) throws IOException {
    	int k = context.getConfiguration().getInt("n", 100);
    	sources = getSources(context);
    	sourceQueues = new ArrayList<TopScoredObjects<Integer>>(sources.length);
    	for (int i = 0; i < sources.length; i++) {
    		sourceQueues.add(new TopScoredObjects<Integer>(k));
    	}
    }

    @Override
    public void reduce(PairOfInts sourceIdNodeId, Iterable<FloatWritable> iterable, Context context)
        throws IOException, InterruptedException {
      Iterator<FloatWritable> iter = iterable.iterator();

      
      	float pageRank = iter.next().get();
      
		for(int i = 0; i < sources.length; i++) {
			if (sourceIdNodeId.getLeftElement() == sources[i]) {
				sourceQueues.get(i).add(sourceIdNodeId.getRightElement(), pageRank);	
			}
		}

		// Shouldn't happen. Throw an exception.
		if (iter.hasNext()) {
			throw new RuntimeException("!! There was more than one page rank in the reducer for source: " 
											+ sourceIdNodeId.getLeftElement()  
											+ ", node:" + sourceIdNodeId.getRightElement());
		}
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    	PairOfInts sourceIdNodeId = new PairOfInts();
    	FloatWritable pageRank = new FloatWritable();
    	
    	for(int i = 0; i < sources.length; i++) {
    		for (PairOfObjectFloat<Integer> nodeIdRank : sourceQueues.get(i).extractAll()) {
    			sourceIdNodeId.set(sources[i], nodeIdRank.getLeftElement());
    			pageRank.set(nodeIdRank.getRightElement());
    			context.write(sourceIdNodeId, pageRank);
    		}
    	}
    }
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("top n").create(TOP));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("sources").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP) || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sources = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);
    LOG.info(" - top: " + n);

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);
    conf.set("Sources", sources);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(PairOfInts.class);
    job.setMapOutputValueClass(FloatWritable.class);

    job.setOutputKeyClass(PairOfInts.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    
    LOG.info("!! Beginning job. !!");
    job.waitForCompletion(true);
    
    Path filePath = new Path(outputPath + "/part-r-00000");
    printPageRanks(job.getConfiguration(), filePath, parseSourceString(sources));

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
  
  public void printPageRanks(Configuration conf, Path filePath, int[] sources) throws URISyntaxException, IOException {
	  FileSystem fs = FileSystem.get(conf);
	  FSDataInputStream in = fs.open(filePath);
	  BufferedReader reader = new BufferedReader(new InputStreamReader(in));
	  
	  try {
		  String line;
		  int currentSource = -1;
		  while((line = reader.readLine()) != null) {
			  String[] splitLine = line.split("[(),\t]");
			  int sourceId = Integer.parseInt(splitLine[1].trim());
			  int nodeId = Integer.parseInt(splitLine[2].trim());
			  float pageRank = Float.parseFloat(splitLine[4].trim());
			  
			  if (sourceId != currentSource) {
				  System.out.println();
				  System.out.println("Source: " + sourceId);
				  currentSource = sourceId;
			  }
			  
			  System.out.println(String.format("%.5f %d", StrictMath.exp(pageRank), nodeId));
		  }
	  }
	  finally {
		  reader.close();
	  }
  }
  
  private int[] parseSourceString(String sourceStr) {
	  String[] sourceStrs = sourceStr.trim().split(",");
	  int[] sourceNums = new int[sourceStrs.length];
	  for (int i = 0; i < sourceStrs.length; i++) {
		  sourceNums[i] = Integer.parseInt(sourceStrs[i]);
	  }
	  return sourceNums;
  }
  
  private static int[] getSources(JobContext context) {
	  int[] sources = context.getConfiguration().getInts("Sources");
	  if (sources == null || sources.length == 0) {
		  throw new RuntimeException("Sources configuration was not found.");
	  }
	  return sources;
  }

}
