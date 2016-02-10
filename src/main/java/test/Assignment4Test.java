package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

import ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment4.FloatArrayStringable;
import ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment4.PageRankNode;

public class Assignment4Test {

	@Test
	public void FloatArrayStringable_Input3Floats_Returns3Floats() {
		float[] ar = new float[3];
		ar[0] = 1;
		ar[1] = 1.12354f;
		ar[2] = -2.53f;
		
		FloatArrayStringable floats = new FloatArrayStringable(3);
		floats.set(ar);
		
		String str = floats.toString();
		FloatArrayStringable floatsOut = new FloatArrayStringable(str);
		
		assertEquals(3, floatsOut.get().length);
		assertTrue(floatsOut.get(0) == ar[0]);
		assertTrue(floatsOut.get(1) == ar[1]);
		assertTrue(floatsOut.get(2) == ar[2]);
	}
	
	@Test
	public void PageRankNode_WriteWithPageRanks_ReadWithPageRanks() throws IOException, ClassNotFoundException {
		float[] prs = new float[3];
		prs[0] = 1;
		prs[1] = 1.12354f;
		prs[2] = -2.53f;
		
		PageRankNode node = new PageRankNode();
		node.setPageRanks(prs);
		node.setType(PageRankNode.Type.Mass);
		
		PageRankNode outNode = ReadAndWriteNode(node);
		
		assertNotNull(outNode);
		assertNotNull(outNode.getPageRanks());
		assertEquals(3, outNode.getPageRanks().length);
		assertTrue(prs[0] == outNode.getPageRanks()[0]);
		assertTrue(prs[1] == outNode.getPageRanks()[1]);
		assertTrue(prs[2] == outNode.getPageRanks()[2]);
	}
	
	private PageRankNode ReadAndWriteNode(PageRankNode node) throws IOException, ClassNotFoundException {
		FileOutputStream fileOut = null;
	 	ObjectOutputStream out = null;
	 	FileInputStream fileIn = null;
	 	ObjectInputStream in = null;
	 	
	 	String testFileName = "testOutput_PageRankNode";
	 	
 		fileOut = new FileOutputStream(testFileName);
		out = new ObjectOutputStream(fileOut);
		node.write(out);
		
		out.close();
		fileOut.close();
		
		fileIn = new FileInputStream(testFileName);
		in = new ObjectInputStream(fileIn);
		PageRankNode outNode = new PageRankNode();
		outNode.readFields(in);
		
		in.close();
 		fileIn.close();
		
		File f = new File(testFileName);
		f.delete();
		
		return outNode;
	}
}
