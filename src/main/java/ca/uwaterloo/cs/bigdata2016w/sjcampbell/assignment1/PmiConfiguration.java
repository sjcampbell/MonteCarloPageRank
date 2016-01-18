package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class PmiConfiguration {
	
	public static final String LineCountProperty = "PmiLineCount";
	
	private Class<?> _pmiClass;
	
	private Configuration _conf;
	
	private Args _args;
	
	public PmiConfiguration(Class<?> pmiClass, Configuration conf, Args args) {
		_pmiClass = pmiClass;
		_conf = conf;
		_args = args;

		logArguments(_args);
	}
	
	public Job SetupWordCountJob(Path intermediatePath) throws IllegalArgumentException, IOException {
		// Job 1 - Word count and total line count
		Job job = Job.getInstance(_conf);
		job.setJobName(StripesPMI.class.getSimpleName() + "_WordCount");
		job.setJarByClass(WordCount.class);
		job.setNumReduceTasks(_args.numReducers);
		FileInputFormat.setInputPaths(job, new Path(_args.input));
		FileOutputFormat.setOutputPath(job, intermediatePath);
		WordCount.ConfigureJob(job);
		
		// Delete the intermediate directory if it exists already.
		FileSystem.get(_conf).delete(intermediatePath, true);
		
		return job;
	}
	
	public long RunWordCountJob(Job job) throws ClassNotFoundException, IOException, InterruptedException {
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		
		Logger log = Logger.getLogger(StripesPMI.class);
		log.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		
		long lineCount = job.getCounters().findCounter(WordCount.Count.LINES).getValue();
		return lineCount;
	}

	// Thanks to @foxroot
	// http://stackoverflow.com/a/30230251/2565692	
	public void AddJobOutputToCache(Configuration config, Job job, Path filePath) throws IOException {
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
	
	// Thanks to @foxroot
	// http://stackoverflow.com/a/30230251/2565692	
	public void addJobOutputToCache(Configuration config, Job job, Path filePath) throws IOException {
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
	
	private void logArguments(Args args) {
		Logger log = Logger.getLogger(StripesPMI.class);
		log.info("Tool: " + _pmiClass.getSimpleName());
		log.info(" - input path: " + args.input);
		log.info(" - output path: " + args.output);
		log.info(" - number of reducers: " + args.numReducers);
	}
}
