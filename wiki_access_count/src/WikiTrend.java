package org.myorg;

import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.lang.NullPointerException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

class Entry {
				public	IntWritable count;
				public	Date day;

				public Entry(Date day, IntWritable count) {
						this.day = day;
						this.count = count;
				}
}


public class WikiTrend {

		/*
		 *  Class to store number of acceses of a page in one day 
		 */
				public static class TrendMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Entry> {
				private  IntWritable num_acc = new IntWritable(0);
				private Text page_name = new Text();

				/* 
				 * ex line structure: en Barack_Obama 997 123091092
				 */
				public void map(LongWritable key, Text value, OutputCollector<Text, Entry> output, Reporter reporter) throws IOException {
						
						String line = value.toString();
						String values[] = line.split(" ");

						page_name.set(values[1]);
						num_acc.set(Integer.parseInt(values[2]));

						Date day = getDateFromInputFile(reporter);


						/* apply filters */
						//if (page_name == null || day == null) return;

						//if (day.compareTo(new Date(2010, 2, 12)) != 0) return;

						//if (values[0] != "en") return;


						/* off we go now */
						Entry e = new Entry(day, num_acc);
						output.collect(page_name, e);
				}


				public static Date getDateFromInputFile(Reporter reporter) {
						
						/* First get the file name */
						FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
						String fileName = fileSplit.getPath().getName();

						return getDateFromFileName(fileName);
				}

				public static Date getDateFromFileName(String fileName) {
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
								return null;
						}

						return date;
				}
		}
/*
		public static class TrendCombiner extends MapReduceBase implements Combiner<Text, Entry, Text, Entry> {
				private  IntWritable num_acc = new IntWritable(0);
				private Text page_name = new Text();

				public void combine(Text key, Entry value, OutputCollector<Text, Entry> output, Reporter reporter) throws IOException {
				}
		}
*/
		public static class TrendReduce extends MapReduceBase implements Reducer<Text, Entry, Text, Text> {
				public void reduce(Text key, Iterator<Entry> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
						int sum = 0;
						while (values.hasNext()) {
								sum += values.next().count.get();
						}
						output.collect(key, new Text(new Integer(sum).toString()));
				}
		}

		public static void main(String[] args) throws Exception {
				if (args.length < 2) {
						System.err.println("must provide in and out files\n");
				}

				JobConf conf = new JobConf(WikiTrend.class);
				conf.setJobName("wiki_trends");

				conf.setOutputKeyClass(Entry.class);
				conf.setOutputValueClass(Text.class);

				conf.setMapOutputValueClass(Entry.class);

				conf.setMapperClass(TrendMap.class);
				conf.setCombinerClass(TrendReduce.class);
				conf.setReducerClass(TrendReduce.class);

				conf.setInputFormat(TextInputFormat.class);
				conf.setOutputFormat(TextOutputFormat.class);

				FileInputFormat.setInputPaths(conf, new Path(args[0]));
				FileOutputFormat.setOutputPath(conf, new Path(args[1]));

				JobClient.runJob(conf);
/*
				String fileName = "pagecounts-20081001-xxxx.gz";

				Date d = TrendMap.getDateFromFileName(fileName);
				System.out.println(d);
				System.out.println(d.getDate());
*/
		}
}

