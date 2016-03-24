package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment7;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

public class BooleanRetrievalHBase extends Configured implements Tool {
	private HTable hTable;
	private FSDataInputStream collection;
	private Stack<Set<Integer>> stack;

	private BooleanRetrievalHBase() {}

	private void initialize(String collectionPath, FileSystem fs) throws IOException {
		collection = fs.open(new Path(collectionPath));
		stack = new Stack<Set<Integer>>();
	}

	private void runQuery(String q) throws IOException {
		String[] terms = q.split("\\s+");

		for (String t : terms) {
			if (t.equals("AND")) {
				performAND();
			} else if (t.equals("OR")) {
				performOR();
			} else {
				pushTerm(t);
			}
		}

		Set<Integer> set = stack.pop();

		for (Integer i : set) {
			String line = fetchLine(i);
			System.out.println(i + "\t" + line);
		}
	}

	private void pushTerm(String term) throws IOException {
		stack.push(fetchDocumentSet(term));
	}

	private void performAND() {
		Set<Integer> s1 = stack.pop();
		Set<Integer> s2 = stack.pop();

		Set<Integer> sn = new TreeSet<Integer>();

		for (int n : s1) {
			if (s2.contains(n)) {
				sn.add(n);
			}
		}

		stack.push(sn);
	}

	private void performOR() {
		Set<Integer> s1 = stack.pop();
		Set<Integer> s2 = stack.pop();

		Set<Integer> sn = new TreeSet<Integer>();

		for (int n : s1) {
			sn.add(n);
		}

		for (int n : s2) {
			sn.add(n);
		}

		stack.push(sn);
	}

	private Set<Integer> fetchDocumentSet(String term) throws IOException {
		Set<Integer> set = new TreeSet<Integer>();

		Get termGet = new Get(Bytes.toBytes(term));
		termGet.addFamily(BuildInvertedIndexHBase.COLFAMILY);
		Result result = hTable.get(termGet);
		NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(BuildInvertedIndexHBase.COLFAMILY);
		
		for(byte[] key : familyMap.keySet()) {
			int docId = Bytes.toInt(key);
			set.add(docId);
		}

		return set;
	}

	public String fetchLine(long offset) throws IOException {
		collection.seek(offset);
		BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

		String d = reader.readLine();
		return d.length() > 80 ? d.substring(0, 80) + "..." : d;
	}

	public static class Args {
		@Option(name = "-table", metaVar = "[name]", required = true, usage = "HBase table to store output")
	    public String table;

	    @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
	    public String config;

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

		initialize(args.collection, fs);
		
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(args.config));
		hTable = new HTable(conf, args.table);
		
		try {
			System.out.println("Query: " + args.query);
			long startTime = System.currentTimeMillis();
			runQuery(args.query);
			System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");
		}
		finally {
			hTable.close();
		}

		return 1;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BooleanRetrievalHBase(), args);
	}
}