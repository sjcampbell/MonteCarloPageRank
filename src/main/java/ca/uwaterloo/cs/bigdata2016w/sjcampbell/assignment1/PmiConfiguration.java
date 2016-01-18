package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment1;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class PmiConfiguration {
	
	public static final String LineCountProperty = "PmiLineCount";
	
	private Class<?> _pmiClass;
	
	private Configuration _conf;
	
	public PmiConfiguration(Class<?> pmiClass, Configuration conf) {
		_pmiClass = pmiClass;
		_conf = conf;
	}
	
	public Job SetupWordCountJob(Path intermediatePath, Args args) throws IllegalArgumentException, IOException {
		logArguments(args);
		
		Job job = Job.getInstance(_conf);
		job.setJobName(StripesPMI.class.getSimpleName() + "_WordCount");
		job.setJarByClass(WordCount.class);
		job.setNumReduceTasks(args.numReducers);
		FileInputFormat.setInputPaths(job, new Path(args.input));
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
	public void AddFilesInPathToJobCache(Configuration config, Job job, Path filePath) throws IOException {
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

	public HashMap<String, Integer> GetDistributedCacheMap(URI[] fileUris) throws IOException {
		HashMap<String, Integer> wordCounts = new HashMap<>();
		
		for(URI fileUri : fileUris) {
			System.out.println("Found cached file: " + fileUri.toString());
			
			if (fileUri.toString().contains("part-r-")) {
				SequenceFile.Reader reader = new SequenceFile.Reader(_conf, Reader.file(new Path(fileUri)));
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
		
		return wordCounts;
	}
	
	private void logArguments(Args args) {
		Logger log = Logger.getLogger(StripesPMI.class);
		log.info("Tool: " + _pmiClass.getSimpleName());
		log.info(" - input path: " + args.input);
		log.info(" - output path: " + args.output);
		log.info(" - number of reducers: " + args.numReducers);
	}
}
