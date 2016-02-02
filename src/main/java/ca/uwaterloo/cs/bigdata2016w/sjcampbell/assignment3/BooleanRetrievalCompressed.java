package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

public class BooleanRetrievalCompressed extends Configured implements Tool {
  private List<MapFile.Reader> indexReaders;
  private FSDataInputStream collection;
  private Stack<DocumentPostings> postingsStack;

  private BooleanRetrievalCompressed() {}

  private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {
	initializeReader(indexPath, fs);
    collection = fs.open(new Path(collectionPath));
    postingsStack = new Stack<DocumentPostings>();
  }
  
  private void initializeReader(String indexPath, FileSystem fs) throws IOException {
	  FileStatus[] fileList = fs.listStatus(new Path(indexPath), 
			  new PathFilter(){
		  		@Override public boolean accept(Path path){
		  			return path.getName().startsWith("part-");
			  } 
	  });
		
	  indexReaders = new ArrayList<MapFile.Reader>();
	  
	  for(FileStatus fileStatus : fileList) {
		  MapFile.Reader reader = new MapFile.Reader(fileStatus.getPath(), fs.getConf());
		  indexReaders.add(reader);
	  }
  }

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTermPostings(t);
      }
    }

    DocumentPostings postings = postingsStack.pop();
    
    Iterator<Integer> iter = postings.docIdsIterator();
    while(iter.hasNext()) {
    	int i = iter.next();
    	if (i < 0) return;
    	String line = fetchLine(i);
    	System.out.println(i + "\t" + line);
    }
  }

  private void pushTermPostings(String term) throws IOException {
	  postingsStack.push(fetchDocPostings(term));
  }

  private void performAND() throws IOException {
	  DocumentPostings p1 = postingsStack.pop();
	  DocumentPostings p2 = postingsStack.pop();
	  
	  DocumentPostings p3 = p1.AND(p2);
	  postingsStack.push(p3);
  }

  private void performOR() throws IOException {
	  DocumentPostings p1 = postingsStack.pop();
	  DocumentPostings p2 = postingsStack.pop();
	  
	  DocumentPostings p3 = p1.OR(p2);
	  postingsStack.push(p3);
  }

  private DocumentPostings fetchDocPostings(String term) throws IOException {
	  Text key = new Text();

	    DocumentPostings docPostings = new DocumentPostings();
	    key.set(term);
	    
	    for (MapFile.Reader reader : indexReaders) {
	    	reader.get(key, docPostings);
	    	if (docPostings != null && docPostings.hasPostings()) {
	    		return docPostings;
	    	}
	    }
	    
	    return null;
  }
  
  private String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

    return reader.readLine();
  }

  public static class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    public String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    public String collection;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    public String query;
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

    if (args.collection.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      return -1;
    }

    FileSystem fs = FileSystem.get(new Configuration());

    initialize(args.index, args.collection, fs);

    System.out.println("Query: " + args.query);
    long startTime = System.currentTimeMillis();
    runQuery(args.query);
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalCompressed(), args);
  }
}
