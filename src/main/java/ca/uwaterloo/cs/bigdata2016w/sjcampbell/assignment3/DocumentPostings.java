package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class DocumentPostings implements Writable {

	public DocumentPostings() { 
		postingsList = new ArrayList<PairOfVInts>();
	}
	
	public DocumentPostings(int docFrequency, ArrayList<PairOfVInts> postings) {
		documentFrequency = docFrequency;
		postingsList = postings;
	}
	
	private int documentFrequency;
	
	private ArrayList<PairOfVInts> postingsList;	
	
	public void addPosting(PairOfVInts posting) {
		postingsList.add(posting);
		documentFrequency++;
	}
	
	public void clear() {
		postingsList.clear();
		documentFrequency = 0;
	}
	
	public boolean hasPostings() {
		return !postingsList.isEmpty();
	}
	
	public void set(int docFrequency, ArrayList<PairOfVInts> postings) {
		documentFrequency = docFrequency;
		postingsList = postings;
	}
	
	public int getDocFrequency() {
		return documentFrequency;
	}
	
	public ArrayList<PairOfVInts> getPostings() {
		return postingsList;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, documentFrequency);
		
		int size = postingsList.size();
		out.writeInt(size);
	    if (size == 0)
	      return;
	    
	    PairOfVInts pair;
	    for (int i = 0; i < size; i++) {
	    	pair = postingsList.get(i);
	    	if (pair == null) {
	    		throw new IOException("Cannot serialize null pairs!");
	    	}
	    	pair.write(out);
	    }
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		documentFrequency = WritableUtils.readVInt(in);
		
		postingsList.clear();

	    int numFields = in.readInt();
	    if (numFields == 0)
	      return;

	    PairOfVInts pair;
	    try {
	      for (int i = 0; i < numFields; i++) {
	    	  pair = new PairOfVInts();
	    	  pair.readFields(in);
	    	  postingsList.add(pair);
	      }
	    } 
	    catch (Exception e) {
	      e.printStackTrace();
	    }
	}
}
