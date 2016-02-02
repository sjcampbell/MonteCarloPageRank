package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment3;

import org.apache.hadoop.io.WritableUtils;

public class VIntMinimal {

	byte[] bytes;

	public VIntMinimal(int i) {
		if (i >= -112 && i <= 127) {
			bytes = new byte[1];
			bytes[0] = (byte)i;
			return;
		}

		int len = -112;
		if (i < 0) {
			i ^= -1L; // take one's complement'
			len = -120;
		}

		long tmp = i;
		while (tmp != 0) {
			tmp = tmp >> 8;
		len--;
		}

		//stream.writeByte((byte)len);
		bytes = new byte[len]; // ??????????
		bytes[0] = (byte)len;
		
		len = (len < -120) ? -(len + 120) : -(len + 112);

		bytes = new byte[len];
		
		for (int idx = len; idx != 0; idx--) {
			int shiftbits = (idx - 1) * 8;
			long mask = 0xFFL << shiftbits;
			//stream.writeByte((byte)((i & mask) >> shiftbits));
			bytes[len - idx] = (byte)((i & mask) >> shiftbits);
		}
	}
	
	public long get() {
		if (bytes.length == 0) return -1;
		
		byte firstByte = bytes[0];
		int len = WritableUtils.decodeVIntSize(firstByte);
		if (len == 1) {
		  return firstByte;
		}
		long i = 0;
		for (int idx = 0; idx < len-1; idx++) {
		  byte b = bytes[idx];
		  i = i << 8;
		  i = i | (b & 0xFF);
		}
		return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
	}
	
	public static int decodeVIntSize(byte value) {
	    if (value >= -112) {
	      return 1;
	    } else if (value < -120) {
	      return -119 - value;
	    }
	    return -111 - value;
	  }
}
