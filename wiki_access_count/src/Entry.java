package org.myorg;

import org.apache.hadoop.io.*;
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


/*
 *  Class to store number of acceses of a page in one day
 */
public class Entry implements Writable {

	/* number of acceses in day represented by date_idx */
	public	long count;

	/* index of day representing an offset from D-Day */
	public	long date_idx;

	public Entry(long date_idx, long count) {
		this.date_idx = date_idx;
		this.count = count;
	}

	public Entry() {
		count = 0;
		date_idx = -1;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(count);
		out.writeLong(date_idx);
	}

	public void readFields(DataInput in) throws IOException {
		count = in.readLong();
		date_idx = in.readLong();
	}
}


