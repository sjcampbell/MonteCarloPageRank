package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

public class Postings {
	public Postings() { 
		postingsBytes = new ByteArrayOutputStream();
		postingsStream = new DataOutputStream(new ByteArrayOutputStream());
	}

	private DataOutputStream postingsStream;
	
	private ByteArrayOutputStream postingsBytes;
	
	private boolean hasPostings = false;
	
	public void addPosting(int docId) throws IOException {
		WritableUtils.writeVInt(postingsStream, docId);
		hasPostings = true;
	}
	
	public void clear() throws IOException {
		postingsStream.flush();
		postingsBytes.reset();
		hasPostings = false;
	}
	
	public boolean hasPostings() {
		return hasPostings;
	}
}
