package org.example.datatypes;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class MyLongWriteable implements Writable{
	long somevalue;
	
	public MyLongWriteable() {
		
	}

	public MyLongWriteable(long i) {
		somevalue = i;
	}

	public long getSomevalue() {
		return somevalue;
	}

	public void setSomevalue(long somevalue) {
		this.somevalue = somevalue;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		somevalue = arg0.readLong();		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(somevalue);		
	}

}
