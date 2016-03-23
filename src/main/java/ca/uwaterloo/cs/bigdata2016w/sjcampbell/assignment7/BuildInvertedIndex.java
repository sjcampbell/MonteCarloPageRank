package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment7;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfStringInt;

public class BuildInvertedIndex extends Configured implements Tool {
	public static final String[] FAMILIES = { "c" };
	public static final byte[] CF = FAMILIES[0].getBytes();
	public static final byte[] COUNT = "count".getBytes();
	
	public static class BuildIndexMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
		private static final PairOfStringInt TERMDOC = new PairOfStringInt();
		private static final IntWritable TERMCOUNT = new IntWritable(); 
		private static final Object2IntFrequencyDistribution<String> COUNTS =
				new Object2IntFrequencyDistributionEntry<String>();

		@Override
		public void map(LongWritable docno, Text doc, Context context)
				throws IOException, InterruptedException {
			String text = doc.toString();

			// Tokenize line.
			List<String> tokens = new ArrayList<String>();
			StringTokenizer itr = new StringTokenizer(text);
			while (itr.hasMoreTokens()) {
				String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
				if (w.length() == 0) continue;
				tokens.add(w);
			}

			// Build a histogram of the terms.
			COUNTS.clear();
			for (String token : tokens) {
				COUNTS.increment(token);
			}

			// Emit postings.
			for (PairOfObjectInt<String> e : COUNTS) {
				TERMDOC.set(e.getLeftElement(), (int)docno.get());
				TERMCOUNT.set(e.getRightElement());
				context.write(TERMDOC, TERMCOUNT);
			}
		}
	}

	public static class BuildIndexReducer extends Reducer<PairOfStringInt, IntWritable, Text, ArrayListWritable<PairOfInts>> {
		private final static Text TERM = new Text();
		private String previousTerm;
		private ArrayListWritable<PairOfInts> docPostings;

		@Override
		public void setup(Context context) throws IOException {
			previousTerm = null;
			docPostings = new ArrayListWritable<PairOfInts>();
		}

		/*	Reducer
		 *  term: key.getLeftElement()
		 *  docId: key.getRightElement()
		 *  term frequency: values[i]
		 */
		@Override
		public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			if (!key.getLeftElement().equals(previousTerm) && previousTerm != null) {
				TERM.set(previousTerm);
				context.write(TERM, docPostings);
				docPostings.clear();
			}

			Iterator<IntWritable> iter = values.iterator();
			int count = 0;
			while(iter.hasNext()) {
				count++;
				if (count > 1) {
					throw new InterruptedException("What happened there? There should only be one value passed into the reducer.");
				}

				IntWritable termCount = iter.next();
				docPostings.add(new PairOfInts(key.getRightElement(), termCount.get()));
			}

			previousTerm = key.getLeftElement();
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			if (previousTerm != null)
			{
				TERM.set(previousTerm);
				context.write(TERM, docPostings);
			}
		}
	}

	public static class IndexPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
		@Override
		public int getPartition(PairOfStringInt key, IntWritable value, int numPartitions) {
			return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		return 0;
	}
}
