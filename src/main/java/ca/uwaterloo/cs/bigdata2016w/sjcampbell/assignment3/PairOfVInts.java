package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class PairOfVInts implements WritableComparable<PairOfVInts> {

	private int leftElement, rightElement;
	
	public PairOfVInts() { }
	
	public PairOfVInts(int left, int right) {
	    set(left, right);
	}
	
	public void set(int left, int right) {
		leftElement = left;
		rightElement = right;
	}
	
	public int getLeftElement() {
		return leftElement;
	}
	
	public int getRightElement() {
		return rightElement;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, leftElement);
		WritableUtils.writeVInt(out, rightElement);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		try {
			leftElement = WritableUtils.readVInt(in);
			rightElement = WritableUtils.readVInt(in);
		}
		catch(Exception e) {
			throw new RuntimeException("Unable to create pair of VInts.");
		}
	}

	@Override
	public int compareTo(PairOfVInts pair) {
		int pairLeft = pair.getLeftElement();
	    int pairRight = pair.getRightElement();

	    if (leftElement == pairLeft) {
	      if (rightElement < pairRight)
	        return -1;
	      if (rightElement > pairRight)
	        return 1;
	      return 0;
	    }

	    if (leftElement < pairLeft)
	      return -1;

	    return 1;
	}

}
