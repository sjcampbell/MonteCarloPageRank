package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class DocumentPostings implements Writable {

	public DocumentPostings() { 
		postingsBuf = new PostingsBuffer();
	}
	
	private PostingsBuffer postingsBuf;

	private int documentFrequency;
	
	private boolean hasPostings = false;
	
	public void addPosting(PairOfVInts posting) throws IOException {
		documentFrequency++;

		// Only keep the document ID. Could keep the term count too if needed later by modifying postingsBuffer.
		postingsBuf.addPosting(posting.getLeftElement());
		
		hasPostings = true;
	}
	
	public void clear() throws IOException {
		postingsBuf.resetReadPos();
		documentFrequency = 0;
	}
	
	public boolean hasPostings() {
		return hasPostings;
	}
	
	public int getDocFrequency() {
		return documentFrequency;
	}

	public PostingsBuffer getBuffer() {
		return postingsBuf;
	}
	
	public Iterator<Integer> andDocumentIds(DocumentPostings docPostings) throws IOException {
		PostingsBuffer intBuffer = postingsBuf.AND(docPostings.getBuffer());
		return new DocIdIterator(intBuffer);
	}
	
	public Iterator<Integer> orDocumentIds(DocumentPostings docPostings) throws IOException {
		PostingsBuffer intBuffer = postingsBuf.OR(docPostings.getBuffer());
		return new DocIdIterator(intBuffer);
	}
	
	public Iterator<Integer> docIdsIterator() {
		return new DocIdIterator(postingsBuf);
	}

	public class DocIdIterator implements Iterator<Integer>
	{
		private PostingsBuffer buffer;
		
		public DocIdIterator(PostingsBuffer buf) {
			buffer = buf;
		}
		
		@Override
		public boolean hasNext() {
			return buffer.canRead();
		}

		@Override
		public Integer next() {
			int docId = -1;
			try {
				docId = WritableUtils.readVInt(buffer);
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
			
			return docId;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();	// Not needed for this assignment
		}
	}

	public void write(DataOutput out) throws IOException {
	/*	WritableUtils.writeVInt(out, documentFrequency);
		
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
	    */
	}

	public void readFields(DataInput in) throws IOException {
		
		/*documentFrequency = WritableUtils.readVInt(in);
		
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
	    */
	}
}
