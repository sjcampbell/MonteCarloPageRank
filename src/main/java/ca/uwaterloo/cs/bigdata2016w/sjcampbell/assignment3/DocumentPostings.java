package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class DocumentPostings implements Writable {

	public DocumentPostings() { 
		postingsBuf = new PostingsBuffer();
	}
	
	public DocumentPostings(PostingsBuffer postingsBuffer) {
		postingsBuf = postingsBuffer;
	}
	
	private PostingsBuffer postingsBuf;

	private int documentFrequency;
	
	public void addPosting(PairOfVInts posting) throws IOException {
		documentFrequency++;

		// Only keep the document ID. Could keep the term count too if needed later by modifying postingsBuffer.
		postingsBuf.addPosting(posting.getLeftElement());
	}
	
	public void clear() throws IOException {
		postingsBuf = new PostingsBuffer();
		documentFrequency = 0;
	}
	
	public boolean hasPostings() {
		return postingsBuf.canRead();
	}
	
	public int getDocFrequency() {
		return documentFrequency;
	}

	public PostingsBuffer getBuffer() {
		return postingsBuf;
	}
	
	public DocumentPostings AND(DocumentPostings postings) throws IOException {
		PostingsBuffer intBuffer = postingsBuf.AND(postings.getBuffer());
		return new DocumentPostings(intBuffer);
	}
	
	public DocumentPostings OR(DocumentPostings postings) throws IOException {
		PostingsBuffer intBuffer = postingsBuf.OR(postings.getBuffer());
		return new DocumentPostings(intBuffer);
	}
	
	public Iterator<Integer> docIdsIterator() {
		return new DocIdIterator(postingsBuf);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, documentFrequency);
		
		int i = WritableUtils.readVInt(postingsBuf);
		while (i != -1) {
			WritableUtils.writeVInt(out, i);
			i = WritableUtils.readVInt(postingsBuf);
		}
	}

	public void readFields(DataInput in) throws IOException {
		documentFrequency = WritableUtils.readVInt(in);
		PostingsBuffer pb = new PostingsBuffer();
		
		try
		{
			int i = WritableUtils.readVInt(in);
			while (i != -1) {
				WritableUtils.writeVInt(pb, i);
				i = WritableUtils.readVInt(in);
			}
			
			postingsBuf = pb;
		} catch(EOFException e) {
			// Done reading
			postingsBuf = pb;
		}
	}
	
	public class DocIdIterator implements Iterator<Integer>
	{
		private PostingsBuffer buffer;
		
		private int previousDocId;
		
		public DocIdIterator(PostingsBuffer buf) {
			buffer = buf;
			previousDocId = 0;
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

			int decompressedDocId = docId + previousDocId; 
			previousDocId += docId;
			return decompressedDocId;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();	// Not needed for this assignment
		}
	}
}
