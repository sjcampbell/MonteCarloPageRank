package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment4;

import java.io.StringWriter;

public class FloatArrayStringable {

	public FloatArrayStringable(int size) {
		 this(size, Float.NEGATIVE_INFINITY);
	}
	
	public FloatArrayStringable(int size, float initValue) {
		floats = new float[size];
		
		for (int i = 0; i < size; i++) {
			floats[i] = initValue;
		}
	}
	
	public FloatArrayStringable(String floatString) {
		if (floatString.length() == 0) {
			floats = new float[0];
			return;
		}
		
		String[] strs = floatString.split(",");
		floats = new float[strs.length];
		for (int i = 0; i < strs.length; i++) {
			floats[i] = Float.parseFloat(strs[i]);
		}
	}
	
	private float[] floats;
	
	public float get(int index) {
		return floats[index];
	}
	
	public float[] get() {
		return floats;
	}
	
	public void set(int index, float value) {
		floats[index] = value;
	}
	
	public void set(float[] fs) {
		floats = fs;
	}
	
	public String toString() {
		StringWriter writer = new StringWriter();
		
		if (floats.length < 1) {
			return "";
		}
		
		writer.write(Float.toString(floats[0]));
		
		for (int i = 1; i < floats.length; i++) {
			writer.write(",");
			writer.write(Float.toString(floats[i]));
		}
		
		return writer.toString();
	}
}
