package org.myorg;

import java.io.IOException;
import java.util.*;
import java.util.Calendar;
import java.util.Vector;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.lang.NullPointerException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.myorg.Entry;

public class WikiTrend {

	/* first date from our wiki stats */
	// TODO - should be received from input
	//public static Date dday = new Date("10/1/2008");
	public static Date dday = new Date("1/1/2010"); /* month day year */

	/* number of sampled days */
	// TODO - should be received from input
	public static final long NO_DAYS = 75; 
	
	public static class TrendMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Entry> {
			private long num_acc;
			private Text page_name = new Text();
			private long date_idx;

			/* 
			 * ex line structure: en Barack_Obama 997 123091092
			 */
			public void map(LongWritable key, Text value, OutputCollector<Text, Entry> output, Reporter reporter) throws IOException {
					String line = value.toString();
					String values[] = line.split(" ");

					page_name.set(values[1]);
					num_acc = Integer.parseInt(values[2]);

					date_idx = getIndexDateFromInputFile(reporter);

					/* apply filters */
					if (page_name == null || date_idx < 0) return;

					//if (values[0] != "en") return;

					/* off we go now */
					Entry e = new Entry(date_idx, num_acc);

					output.collect(page_name, e);
			}


			public static long getIndexDateFromInputFile(Reporter reporter) {
					
					/* First get the file name */
					FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
					String fileName = fileSplit.getPath().getName();

					return getIndexDateFromFileName(fileName);
			}

			public static long getIndexDateFromFileName(String fileName) {
					/*
					 * File names are in the form of pagecounts-20081001-xxxx.gz
					 *
					 * We want to extract the date represented by 
					 * (year, month, day)
					 */

					Date date = null;
					SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
					try {
						date = df.parse(fileName, new ParsePosition(11));
					} catch (NullPointerException nex) {
							return -1;
					}

					/* compute difference */
					return daysFromDDay(date);
			}

			/* XXX - could this be any better and still be correct ? */
			public static long daysFromDDay(Date day) {
					/* init calendars */
					Calendar endDate	= Calendar.getInstance();	endDate.setTime(day);
					Calendar date 		= Calendar.getInstance();	date.setTime(dday);

					long daysBetween = 0;
					while (date.before(endDate)) {
							date.add(Calendar.DAY_OF_MONTH, 1);
							daysBetween++;
					}

					return daysBetween;
			}
	}

	public static class TrendCombine extends MapReduceBase implements Reducer<Text, Entry, Text, Entry> {
			public void reduce(Text key, Iterator<Entry> values, OutputCollector<Text, Entry> output, Reporter reporter) throws IOException {
				while (values.hasNext()) {
					output.collect(key, values.next());
				}
			}
	}

	public static class TrendReduce extends MapReduceBase implements Reducer<Text, Entry, Text, Text> {
			//private static Vector<String> acc_count = new Vector<String>((int)NO_DAYS);
			//private static String[] acc_count = new String[(int)NO_DAYS];
			private static long[] acc_count;

			public void reduce(Text key, Iterator<Entry> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
					int sum = 0;
					long max = 0;

					acc_count = new long[(int) NO_DAYS];

					while (values.hasNext()) {
							Entry e = values.next();
							acc_count[(int)e.date_idx] += e.count;
					}
					for (int i = 0; i < acc_count.length; i++)
						if (max < acc_count[i])
							max = acc_count[i];
	
					if (max > 1000)
						output.collect(key, new Text( printExisting() ));
			}

			public static String printAll() {
					String result_line = new String();
					for (int i = 0; i < NO_DAYS; i++)
							result_line += acc_count[i] + " ";

					return result_line;
			}

			public static String printExisting() {
					String result_line = new String();
					for (int i = 0; i < NO_DAYS; i++)
							if (acc_count[i] > 0)
								result_line += i + "-" + acc_count[i] + " ";

					return result_line;
			}

	}

	public static void computeTrend(Path inPath, Path outPath) throws Exception {
			JobConf conf = new JobConf(WikiTrend.class);
			conf.setJobName("wiki_trends");

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			conf.setMapOutputValueClass(Entry.class);

			conf.setMapperClass(TrendMap.class);
			conf.setCombinerClass(TrendCombine.class);
			conf.setReducerClass(TrendReduce.class);

			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, inPath);
			FileOutputFormat.setOutputPath(conf, outPath);

			JobClient.runJob(conf);
	}
	

	public static void main(String[] args) throws Exception {
			if (args.length < 2) {
					System.err.println("must provide in and out files\n");
			}
		
			computeTrend(new Path(args[0]), new Path(args[1]));

			//testDate();
	}

	public static void testDate() {
			String fileName = "pagecounts-20081001-xxxx.gz";

			long d = TrendMap.getIndexDateFromFileName(fileName);
			System.out.println(d);
	}

}
