package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment7;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.pair.PairOfStringInt;

public class BuildInvertedIndexHBase extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(BuildInvertedIndexHBase.class);

	public static final String[] FAMILIES = { "p" };
	public static final byte[] COLFAMILY = FAMILIES[0].getBytes();

	private BuildInvertedIndexHBase() {}

	public static class Args {
	    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
	    public String input;

	    @Option(name = "-table", metaVar = "[name]", required = true, usage = "HBase table to store output")
	    public String table;

	    @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
	    public String config;

	    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
	    public int numReducers = 1;
	}
	
	public static class MyTableReducer extends TableReducer<PairOfStringInt, IntWritable, ImmutableBytesWritable> {
		
		public void reduce(PairOfStringInt termDoc, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

			if (!values.iterator().hasNext()) {
				throw new RuntimeException("Term frequency was missing for term: " + termDoc.getLeftElement());
			}
			
			// row key: term (left element)
			Put put = new Put(Bytes.toBytes(termDoc.getLeftElement().toString()));
			
			int termFrequency = values.iterator().next().get();
			
			// add(family, column qualifier, value)
			// column family: "p"
			// column qualifier: document ID
			// value: term frequency
			put.add(COLFAMILY, Bytes.toBytes(termDoc.getRightElement()), Bytes.toBytes(termFrequency));
		
			context.write(null, put);
		}
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

		LOG.info("Tool: " + BuildInvertedIndexHBase.class.getSimpleName());
		LOG.info(" - input path: " + args.input);
		LOG.info(" - output table: " + args.table);
		LOG.info(" - config: " + args.config);
		LOG.info(" - reducers: " + args.numReducers);

		Configuration conf = getConf();
		conf.addResource(new Path(args.config));
		createHBaseTable(conf, args.table);
		
		Job job = Job.getInstance(conf);
		configureJob(job, args.input, args.numReducers);
		FileInputFormat.setInputPaths(job, new Path(args.input));	
		TableMapReduceUtil.initTableReducerJob(args.table, MyTableReducer.class, job);
		
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		return 0;
	}
	
	private static void createHBaseTable(Configuration conf, String tableName) throws IOException {
		Configuration hbaseConfig = HBaseConfiguration.create(conf);
		HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
		if (admin.tableExists(tableName)) {
			LOG.info(String.format("Table '%s' exists: dropping table and recreating.", tableName));
		    LOG.info(String.format("Disabling table '%s'", tableName));
		    admin.disableTable(tableName);
		    LOG.info(String.format("Droppping table '%s'", tableName));
		    admin.deleteTable(tableName);
		}
		
		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
	    for (int i = 0; i < FAMILIES.length; i++) {
	    	HColumnDescriptor hColumnDesc = new HColumnDescriptor(FAMILIES[i]);
	    	tableDesc.addFamily(hColumnDesc);
	    }
	    
	    admin.createTable(tableDesc);
	    LOG.info(String.format("Successfully created table '%s'", tableName));
	    
	    admin.close();
	}
	
	private static void configureJob(Job job, String input, int numReducers) throws IllegalArgumentException, IOException {
		job.setJobName(BuildInvertedIndexHBase.class.getSimpleName());
		job.setJarByClass(BuildInvertedIndexHBase.class);
		job.setNumReduceTasks(numReducers);

		job.setMapOutputKeyClass(PairOfStringInt.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(BuildInvertedIndex.BuildIndexMapper.class);
		job.setPartitionerClass(BuildInvertedIndex.IndexPartitioner.class);
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BuildInvertedIndexHBase(), args);
	}
}