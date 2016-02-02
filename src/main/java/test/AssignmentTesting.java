package test;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;

import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;

import ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3.DocumentPostings;
import ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3.PairOfVInts;
import ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3.PostingsBuffer;

public class AssignmentTesting {
	
	@Test
	public void PairOfVIntsSerialization() throws IOException, ClassNotFoundException {
		FileOutputStream fileOut = new FileOutputStream("testOutput");
	 	ObjectOutputStream out = new ObjectOutputStream(fileOut);
	 	
	 	PairOfVInts pair = new PairOfVInts(1, 2);

	 	pair.write(out);
	 	
	 	out.close();
	 	fileOut.close();
	
	 	FileInputStream fileIn = new FileInputStream("testOutput");
	 	ObjectInputStream in = new ObjectInputStream(fileIn);
	 	
	 	PairOfVInts inPair = new PairOfVInts();
	 	inPair.readFields(in);
	 	
	 	in.close();
	 	fileIn.close();
	}
	
	@Test
	public void DocPostingsSerialization_OneDoc() throws IOException, ClassNotFoundException {
		FileOutputStream fileOut = null;
	 	ObjectOutputStream out = null;
	 	FileInputStream fileIn = null;
	 	ObjectInputStream in = null;
	 	
	 	try {
	 		fileOut = new FileOutputStream("testOutput_docPostings");
	 		out = new ObjectOutputStream(fileOut);
	 		
		 	DocumentPostings docPostings = new DocumentPostings();
		 	PairOfVInts pair = new PairOfVInts(17, 2);
		 	docPostings.addPosting(pair);
		 	docPostings.write(out);
		 	
		 	out.close();
		 	fileOut.close();
		
		 	fileIn = new FileInputStream("testOutput_docPostings");
		 	in = new ObjectInputStream(fileIn);

		 	DocumentPostings inDocPostings = new DocumentPostings();
		 	inDocPostings.readFields(in);
		 	
		 	assertNotNull(inDocPostings);
		 	
		 	Iterator<Integer> docIds = inDocPostings.docIdsIterator();
		 	int count = 0;
			int [] resultIds = new int[10]; 
			while(docIds.hasNext() && count < 10) {
				resultIds[count++] = docIds.next();
			}
		 	
			assertEquals(1, count);
			assertEquals(17, resultIds[0]);
		 	assertEquals(1, inDocPostings.getDocFrequency());
		 	
		 	in.close();
		 	fileIn.close();
	 	}
	 	finally {
	 		if (out != null) out.close();
	 		if (fileOut != null) fileOut.close();
	 		
	 		if (in != null) in.close();
	 		if (fileIn != null) fileIn.close();
	 	}
	}

	@Test
	public void DocPostingsSerialization_ThreeDocs() throws IOException, ClassNotFoundException {
		FileOutputStream fileOut = null;
	 	ObjectOutputStream out = null;
	 	FileInputStream fileIn = null;
	 	ObjectInputStream in = null;
	 	
	 	try {
	 		fileOut = new FileOutputStream("testOutput_docPostings");
	 		out = new ObjectOutputStream(fileOut);
	 		
		 	DocumentPostings docPostings = new DocumentPostings();
		 	PairOfVInts pair1 = new PairOfVInts(100, 2);	// 100
		 	PairOfVInts pair2 = new PairOfVInts(3, 2);		// 103
		 	PairOfVInts pair3 = new PairOfVInts(123, 2);	// 226
		 	docPostings.addPosting(pair1);
		 	docPostings.addPosting(pair2);
		 	docPostings.addPosting(pair3);
		 	docPostings.write(out);
		 	
		 	out.close();
		 	fileOut.close();
		
		 	fileIn = new FileInputStream("testOutput_docPostings");
		 	in = new ObjectInputStream(fileIn);

		 	DocumentPostings inDocPostings = new DocumentPostings();
		 	inDocPostings.readFields(in);
		 	
		 	assertNotNull(inDocPostings);
		 	
		 	Iterator<Integer> docIds = inDocPostings.docIdsIterator();
		 	int count = 0;
			int [] resultIds = new int[10]; 
			while(docIds.hasNext() && count < 10) {
				resultIds[count++] = docIds.next();
			}
		 	
			assertEquals(3, count);
			assertEquals(100, resultIds[0]);
			assertEquals(103, resultIds[1]);
			assertEquals(226, resultIds[2]);
		 	assertEquals(3, inDocPostings.getDocFrequency());
		 	
		 	in.close();
		 	fileIn.close();
	 	}
	 	finally {
	 		if (out != null) out.close();
	 		if (fileOut != null) fileOut.close();
	 		
	 		if (in != null) in.close();
	 		if (fileIn != null) fileIn.close();
	 	}
	}

	@Test
	public void DocPostingsSerialization_LargeVInts() throws IOException, ClassNotFoundException {
		FileOutputStream fileOut = null;
	 	ObjectOutputStream out = null;
	 	FileInputStream fileIn = null;
	 	ObjectInputStream in = null;
	 	
	 	try {
	 		fileOut = new FileOutputStream("testOutput_docPostings");
	 		out = new ObjectOutputStream(fileOut);
	 		
	 		int pairL = 1012023040; // ~1 billion
	 		int pairR = 2000003421;
	 		
		 	DocumentPostings docPostings = new DocumentPostings();
		 	PairOfVInts pair1 = new PairOfVInts(pairL, pairR);
		 	PairOfVInts pair2 = new PairOfVInts(1, pairR);
		 	docPostings.addPosting(pair1);
		 	docPostings.addPosting(pair2);
		 	docPostings.write(out);
		 	
		 	out.close();
		 	fileOut.close();
		
		 	fileIn = new FileInputStream("testOutput_docPostings");
		 	in = new ObjectInputStream(fileIn);

		 	DocumentPostings inDocPostings = new DocumentPostings();
		 	inDocPostings.readFields(in);
		 	
		 	assertNotNull(inDocPostings);
		 	
		 	Iterator<Integer> docIds = inDocPostings.docIdsIterator();
		 	int count = 0;
			int [] resultIds = new int[10]; 
			while(docIds.hasNext() && count < 10) {
				resultIds[count++] = docIds.next();
			}
		 	
			assertEquals(2, count);
			assertEquals(pairL, resultIds[0]);
			assertEquals(pairL + 1, resultIds[1]);
		 	assertEquals(inDocPostings.getDocFrequency(), 2);
		 	
		 	in.close();
		 	fileIn.close();
	 	}
	 	finally {
	 		if (out != null) out.close();
	 		if (fileOut != null) fileOut.close();
	 		
	 		if (in != null) in.close();
	 		if (fileIn != null) fileIn.close();
	 	}
	}
	
	@Test 
	public void TestPostingsBufferVIntsSerialization() throws IOException{
		PostingsBuffer buf = new PostingsBuffer();

		WritableUtils.writeVInt(buf, 3);
		WritableUtils.writeVInt(buf, 2);
		WritableUtils.writeVInt(buf, 1);
		WritableUtils.writeVInt(buf, 1234);
		WritableUtils.writeVInt(buf, 123456789);
		
		assertEquals(3, WritableUtils.readVInt(buf));
		assertEquals(2, WritableUtils.readVInt(buf));
		assertEquals(1, WritableUtils.readVInt(buf));
		assertEquals(1234, WritableUtils.readVInt(buf));
		assertEquals(123456789, WritableUtils.readVInt(buf));
	}
	
	@Test
	public void DocumentPostingsOR() throws IOException {
		DocumentPostings docPostings1 = new DocumentPostings();
		docPostings1.addPosting(new PairOfVInts(100, 7));	// 100
		docPostings1.addPosting(new PairOfVInts(1, 7));		// 101
		
		DocumentPostings docPostings2 = new DocumentPostings();
		docPostings2.addPosting(new PairOfVInts(101, 9));	// 101
		docPostings2.addPosting(new PairOfVInts(2, 9));		// 103
		
		DocumentPostings d3 = docPostings1.OR(docPostings2);
		Iterator<Integer> docIds = d3.docIdsIterator();
		
		assertNotNull(docIds);
		assertTrue(docIds.hasNext());
		
		int count = 0;
		int [] resultIds = new int[10]; 
		while(docIds.hasNext() && count < 10) {
			resultIds[count++] = docIds.next();
		}

		assertEquals(3, count);
		assertEquals(100, resultIds[0]);
		assertEquals(101, resultIds[1]);
		assertEquals(103, resultIds[2]);
	}
	
	@Test
	public void DocumentPostingsOR2() throws IOException {
		DocumentPostings docPostings1 = new DocumentPostings();
		docPostings1.addPosting(new PairOfVInts(103, 7));	// 103
		docPostings1.addPosting(new PairOfVInts(2, 7));		// 105
		docPostings1.addPosting(new PairOfVInts(10000, 7));	// 10105
		
		DocumentPostings docPostings2 = new DocumentPostings();
		docPostings2.addPosting(new PairOfVInts(101, 9));	// 101
		docPostings2.addPosting(new PairOfVInts(2, 9));		// 103
		docPostings2.addPosting(new PairOfVInts(4, 9));		// 107
		docPostings2.addPosting(new PairOfVInts(100, 9));	// 207
		
		DocumentPostings d3 = docPostings1.OR(docPostings2);
		Iterator<Integer> docIds = d3.docIdsIterator();
		
		assertNotNull(docIds);
		assertTrue(docIds.hasNext());
		
		int count = 0;
		int [] resultIds = new int[10]; 
		while(docIds.hasNext() && count < 10) {
			resultIds[count++] = docIds.next();
		}

		assertEquals(6, count);
		assertEquals(101, resultIds[0]);
		assertEquals(103, resultIds[1]);
		assertEquals(105, resultIds[2]);
		assertEquals(107, resultIds[3]);
		assertEquals(207, resultIds[4]);
		assertEquals(10105, resultIds[5]);
	}
	
	@Test
	public void DocumentPostingsAND() throws IOException {
		DocumentPostings docPostings1 = new DocumentPostings();
		docPostings1.addPosting(new PairOfVInts(100, 7));	// 100
		docPostings1.addPosting(new PairOfVInts(1, 7));		// 101
		docPostings1.addPosting(new PairOfVInts(3, 7));		// 104
		
		DocumentPostings docPostings2 = new DocumentPostings();
		docPostings2.addPosting(new PairOfVInts(101, 9));	// 101
		docPostings2.addPosting(new PairOfVInts(2, 9));		// 103
		docPostings2.addPosting(new PairOfVInts(1, 9));		// 104
		
		DocumentPostings d3 = docPostings1.AND(docPostings2);
		Iterator<Integer> docIds = d3.docIdsIterator();
		
		assertNotNull(docIds);
		assertTrue(docIds.hasNext());
		
		int count = 0;
		int [] resultIds = new int[10]; 
		while(docIds.hasNext() && count < 10) {
			resultIds[count++] = docIds.next();
		}

		assertEquals(2, count);
		assertEquals(101, resultIds[0]);
		assertEquals(104, resultIds[1]);
	}
	
	@Test
	public void DocumentPostingsAND2() throws IOException {
		DocumentPostings docPostings1 = new DocumentPostings();
		docPostings1.addPosting(new PairOfVInts(200, 7));	// 200
		docPostings1.addPosting(new PairOfVInts(1, 7));		// 201
		docPostings1.addPosting(new PairOfVInts(2, 7));		// 203
		docPostings1.addPosting(new PairOfVInts(1, 7));		// 204
		docPostings1.addPosting(new PairOfVInts(1, 7));		// 205
		
		DocumentPostings docPostings2 = new DocumentPostings();
		docPostings2.addPosting(new PairOfVInts(101, 9));	// 101
		docPostings2.addPosting(new PairOfVInts(2, 9));		// 103
		docPostings2.addPosting(new PairOfVInts(100, 9));	// 203
		docPostings2.addPosting(new PairOfVInts(2, 9));		// 205
		
		DocumentPostings d3 = docPostings1.AND(docPostings2);
		Iterator<Integer> docIds = d3.docIdsIterator();
		
		assertNotNull(docIds);
		assertTrue(docIds.hasNext());
		
		int count = 0;
		int [] resultIds = new int[10]; 
		while(docIds.hasNext() && count < 10) {
			resultIds[count++] = docIds.next();
		}

		assertEquals(2, count);
		assertEquals(203, resultIds[0]);
		assertEquals(205, resultIds[1]);
	}
}







































