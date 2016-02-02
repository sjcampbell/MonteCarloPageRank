package test;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Iterator;

import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;

import ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3.DocumentPostings;
import ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3.PairOfVInts;
import ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3.PostingsBuffer;
import tl.lin.data.pair.PairOfInts;

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
	
	/*
	@Test
	public void DocPostingsSerialization_OnePair() throws IOException, ClassNotFoundException {
		FileOutputStream fileOut = null;
	 	ObjectOutputStream out = null;
	 	FileInputStream fileIn = null;
	 	ObjectInputStream in = null;
	 	
	 	try {
	 		fileOut = new FileOutputStream("testOutput_docPostings");
	 		out = new ObjectOutputStream(fileOut);
	 		
		 	DocumentPostings docPostings = new DocumentPostings();
		 	PairOfVInts pair = new PairOfVInts(1, 2);
		 	docPostings.addPosting(pair);
		 	docPostings.write(out);
		 	
		 	out.close();
		 	fileOut.close();
		
		 	fileIn = new FileInputStream("testOutput_docPostings");
		 	in = new ObjectInputStream(fileIn);

		 	DocumentPostings inDocPostings = new DocumentPostings();
		 	inDocPostings.readFields(in);
		 	
		 	assertEquals(inDocPostings.getDocFrequency(), 1);
		 	assertNotNull(inDocPostings.getPostings());
		 	assertEquals(inDocPostings.getPostings().size(), 1);
		 	assertEquals(inDocPostings.getPostings().get(0).getLeftElement(), 1);
		 	assertEquals(inDocPostings.getPostings().get(0).getRightElement(), 2);
		 	
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
		 	PairOfVInts pair = new PairOfVInts(pairL, pairR);
		 	docPostings.addPosting(pair);
		 	docPostings.write(out);
		 	
		 	out.close();
		 	fileOut.close();
		
		 	fileIn = new FileInputStream("testOutput_docPostings");
		 	in = new ObjectInputStream(fileIn);

		 	DocumentPostings inDocPostings = new DocumentPostings();
		 	inDocPostings.readFields(in);
		 	
		 	assertEquals(inDocPostings.getDocFrequency(), 1);
		 	assertNotNull(inDocPostings.getPostings());
		 	assertEquals(1, inDocPostings.getPostings().size());
		 	assertEquals(pairL, inDocPostings.getPostings().get(0).getLeftElement());
		 	assertEquals(pairR, inDocPostings.getPostings().get(0).getRightElement());
		 	
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
	*/
	
	@Test 
	public void TestPostingsBufferVInts() throws IOException{
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
		
		Iterator<Integer> docIds = docPostings1.orDocumentIds(docPostings2);
		
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
	
	public void DocumentPostingsOR2() throws IOException {
		DocumentPostings docPostings1 = new DocumentPostings();
		docPostings1.addPosting(new PairOfVInts(103, 7));	// 103
		docPostings1.addPosting(new PairOfVInts(2, 7));		// 105
		docPostings1.addPosting(new PairOfVInts(10000, 7));	// 10105
		
		DocumentPostings docPostings2 = new DocumentPostings();
		docPostings2.addPosting(new PairOfVInts(101, 9));	// 101
		docPostings2.addPosting(new PairOfVInts(2, 9));		// 103
		docPostings2.addPosting(new PairOfVInts(4, 9));		// 105
		docPostings2.addPosting(new PairOfVInts(100, 9));	// 205
		
		Iterator<Integer> docIds = docPostings1.orDocumentIds(docPostings2);
		
		assertNotNull(docIds);
		assertTrue(docIds.hasNext());
		
		int count = 0;
		int [] resultIds = new int[10]; 
		while(docIds.hasNext() && count < 10) {
			resultIds[count++] = docIds.next();
		}

		assertEquals(5, count);
		assertEquals(101, resultIds[0]);
		assertEquals(103, resultIds[1]);
		assertEquals(105, resultIds[2]);
		assertEquals(205, resultIds[3]);
		assertEquals(10105, resultIds[4]);
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
		
		Iterator<Integer> docIds = docPostings1.andDocumentIds(docPostings2);
		
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
}







































