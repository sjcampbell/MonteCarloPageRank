package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableUtils;

// It's proving somewhat awkward to work with DataInput and DataOutput in memory, 
// so here's a quick and dirty implementation that only implements the two methods I need.
public class PostingsBuffer implements DataOutput, DataInput {

	public PostingsBuffer()
	{
		buffer = new byte[32];
	}
	
	private byte[] buffer;

	private int readPos = 0;
	
	private int writePos = 0;
	
	@Override
	public void writeByte(int v) throws IOException {
		int newcount = writePos + 1;
		if (newcount > buffer.length) {
			buffer = Arrays.copyOf(buffer, Math.max(buffer.length << 1, newcount));
		}
		
		buffer[writePos] = (byte)v;
		writePos = newcount;
	}
	
	@Override
	public byte readByte() throws IOException {
		if (readPos < writePos) {
			return buffer[readPos++];
		}
		else {
			return -1;
		}
	}
	
	public void resetReadPos() {
		readPos = 0;
	}
	
	public void clear() {
		buffer = new byte[32];
		readPos = 0;
		writePos = 0;
	}

	public boolean canRead() {
		return readPos < writePos;
	}
	
	public void addPosting(int docId) throws IOException {
		WritableUtils.writeVInt(this, docId);
	}
	
	// Assumption: postings are already sorted.
	public PostingsBuffer AND(PostingsBuffer postings) throws IOException {
		PostingsBuffer newBuf = new PostingsBuffer();  
		
		long thisDocId = 0;
		long thisGap = WritableUtils.readVInt(this);
		long thatDocId = 0;
		long thatGap = WritableUtils.readVInt(postings);
		
		while(thisDocId > -1 || thatDocId > -1) {
			if (thisDocId < 0 || ((thatDocId > -1) && (thatDocId + thatGap) < (thisDocId + thisGap))) {
				thatGap += thatDocId;
				thatDocId = WritableUtils.readVLong(postings);
			}
			else if (thatDocId < 0 || ((thisDocId > -1) && (thisDocId + thisGap) < (thatDocId + thatGap))) {
				thisGap += thisDocId;
				thisDocId = WritableUtils.readVLong(this);				
			}
			else {	
				// doc IDs are equal
				WritableUtils.writeVLong(newBuf, thisDocId + thisGap);
				thisGap += thisDocId;
				thatGap += thatDocId;

				thisDocId = WritableUtils.readVLong(this);
				thatDocId = WritableUtils.readVLong(postings);
			}
		}
		
		return newBuf;
	}
	
	// This method assumes postings are already sorted.
	public PostingsBuffer AND2(PostingsBuffer postings) throws IOException {
		
		PostingsBuffer newBuf = new PostingsBuffer();
		
		long thisGap = WritableUtils.readVLong(this);
		long thatGap = WritableUtils.readVLong(postings);
		long runningGap = Math.min(thisGap, thatGap);
		long gapSum = runningGap;
		
		while(thisGap > -1 || thatGap > -1) {
			if (thisGap == thatGap) {
				WritableUtils.writeVLong(newBuf, gapSum);
				thisGap = WritableUtils.readVLong(this);
				thatGap = WritableUtils.readVLong(postings);
				gapSum = 0;
			}
			else if (runningGap == thisGap) {
				thisGap = WritableUtils.readVLong(this);
				thatGap -= runningGap;
			}
			else if (runningGap == thatGap) {
				thatGap = WritableUtils.readVLong(postings);
				thisGap -= runningGap;
			}
			
			if (thisGap < 0) {
				runningGap = thatGap;
			}
			else if (thatGap < 0) {
				runningGap = thisGap;
			}
			else {
				runningGap = (thisGap <= thatGap) ? thisGap : thatGap;
			}
			
			gapSum += runningGap;
		}
		
		return newBuf;
	}
	
	// This method assumes postings are already sorted.
	public PostingsBuffer OR(PostingsBuffer postings) throws IOException {
		
		PostingsBuffer newBuf = new PostingsBuffer();
		
		long thisGap = WritableUtils.readVLong(this);
		long thatGap = WritableUtils.readVLong(postings);
		long runningGap = Math.min(thisGap, thatGap);
		
		while(thisGap > -1 || thatGap > -1) {

			if (thisGap == thatGap) {
				WritableUtils.writeVLong(newBuf, thisGap);
				thisGap = WritableUtils.readVLong(this);
				thatGap = WritableUtils.readVLong(postings);
			}
			else if (runningGap == thisGap) {
				WritableUtils.writeVLong(newBuf, thisGap);
				thisGap = WritableUtils.readVLong(this);
				thatGap -= runningGap;
			}
			else if (runningGap == thatGap) {
				WritableUtils.writeVLong(newBuf, thatGap);
				thatGap = WritableUtils.readVLong(postings);
				thisGap -= runningGap;
			}
			
			if (thisGap < 0) {
				runningGap = thatGap;
			}
			else if (thatGap < 0) {
				runningGap = thisGap;
			}
			else {
				runningGap = (thisGap <= thatGap) ? thisGap : thatGap;
			}
		}
		
		return newBuf;
	}

	//
	// Ignore methods below unless they become useful.
	// ===============================================

	@Override
	public void write(int b) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void write(byte[] b) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeBytes(String s) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeChar(int v) throws IOException {
		throw new UnsupportedOperationException();
		
	}

	@Override
	public void writeChars(String s) throws IOException {
		throw new UnsupportedOperationException();
		
	}

	@Override
	public void writeDouble(double v) throws IOException {
		throw new UnsupportedOperationException();
		
	}

	@Override
	public void writeFloat(float v) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeInt(int v) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeLong(long v) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeShort(int v) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeUTF(String s) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean readBoolean() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public char readChar() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public double readDouble() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public float readFloat() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void readFully(byte[] b) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		throw new UnsupportedOperationException();		
	}

	@Override
	public int readInt() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String readLine() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long readLong() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public short readShort() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String readUTF() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int readUnsignedByte() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int readUnsignedShort() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int skipBytes(int n) throws IOException {
		throw new UnsupportedOperationException();
	}
}
