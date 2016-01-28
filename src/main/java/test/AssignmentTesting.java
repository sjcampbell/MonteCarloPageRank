package test;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

import ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3.DocumentPostings;
import ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3.PairOfVInts;

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
		 	assertEquals(inDocPostings.getPostings().size(), 1);
		 	assertEquals(inDocPostings.getPostings().get(0).getLeftElement(), pairL);
		 	assertEquals(inDocPostings.getPostings().get(0).getRightElement(), pairR);
		 	
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
}
