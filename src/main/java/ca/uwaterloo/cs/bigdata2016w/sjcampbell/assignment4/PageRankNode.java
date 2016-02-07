package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import tl.lin.data.array.ArrayListOfIntsWritable;

/**
 * Representation of a graph node for PageRank.
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class PageRankNode implements Writable {
  public static enum Type {
    Complete((byte) 0),  // PageRank mass and adjacency list.
    Mass((byte) 1),      // PageRank mass only.
    Structure((byte) 2); // Adjacency list only.

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  };

  private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

  private Type type;
  private int nodeid;
  private float pagerank;
  private ArrayListOfIntsWritable adjacenyList;
  private float[] pageranks;

  public PageRankNode() {}

  public void setPageRanks(float[] p) {
	  this.pageranks = p;
  }
  
  public float[] getPageRanks() {
	  return this.pageranks;
  }
  
  public void setPageRankValues(float value) {
	  for (int i = 0; i < pageranks.length; i++) {
		  pageranks[i] = value;
	  }
  }

  public int getNodeId() {
    return nodeid;
  }

  public void setNodeId(int n) {
    this.nodeid = n;
  }

  public ArrayListOfIntsWritable getAdjacenyList() {
    return adjacenyList;
  }

  public void setAdjacencyList(ArrayListOfIntsWritable list) {
    this.adjacenyList = list;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  /**
   * Deserializes this object.
   *
   * @param in source for raw byte representation
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int b = in.readByte();
    type = mapping[b];
    nodeid = in.readInt();

    if (type.equals(Type.Mass)) {
    	pageranks = readPageRanks(in);
    	return;
    }

    if (type.equals(Type.Complete)) {
    	pageranks = readPageRanks(in);
    }

    adjacenyList = new ArrayListOfIntsWritable();
    adjacenyList.readFields(in);
  }
  
  private float[] readPageRanks(DataInput in) throws IOException {
	  int length = in.readInt();
	  float[] ranks = new float[length];
	  for (int i = 0; i < length; i++) {
		  ranks[i] = in.readFloat();
	  }
	  return ranks;
  }

  /**
   * Serializes this object.
   *
   * @param out where to write the raw byte representation
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(type.val);
    out.writeInt(nodeid);

    if (type.equals(Type.Mass)) {
      writePageRanks(pageranks, out);
      return;
    }

    if (type.equals(Type.Complete)) {
    	writePageRanks(pageranks, out);
    }

    adjacenyList.write(out);
  }
  
  private void writePageRanks(float[] ranks, DataOutput out) throws IOException {
	  out.writeInt(ranks.length);
	  for (int i = 0; i < ranks.length; i++) {
		  out.writeFloat(ranks[i]);
	  }
  }

  @Override
  public String toString() {
    return String.format("{%d %.4f %s}", nodeid, pagerank, (adjacenyList == null ? "[]"
        : adjacenyList.toString(10)));
  }

  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNode create(DataInput in) throws IOException {
    PageRankNode m = new PageRankNode();
    m.readFields(in);

    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNode create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
