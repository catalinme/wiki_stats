package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class EditViewBalance {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			output.collect(key, value);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, Text, Text> {
	
		static int line = 0;
	
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
			String totalValues = "";
			
			while (values.hasNext()) {
				totalValues += values.next().toString();
			}
			
			line++;
			
			output.collect(new Text("!@##@! " + totalValues), new Text("" + line));
		}
	}
	
	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		/* 
		 * line structure: "revision_id page_id text_id comment user_id user_name timestamp revision_minor 0 NULL NULL"
		 */
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] values = line.split("\\s+");
			
			for (int i = 0; i < values.length; i++)
					System.out.println("<" + values[i] + ">");
					
			//System.out.println(values[2525]);
			
			if (values[0].equals("!@##@!")) {
			
				LongWritable id = new LongWritable(Long.parseLong(values[values.length - 1]));
				String title = "";
				
				for (int i = 1; i < values.length - 1; i++) {
					title += values[i] + " ";
				}
				
				System.out.println("!KEY: " + id + " ||| VALUE: " + new Text("!@##@!" + title.substring(0, title.length() - 1)));
				//System.out.println(values[25]);

				output.collect(id, new Text("!@##@!" + title.substring(0, title.length() - 1)));
			} else {
				
				LongWritable id = new LongWritable(Long.parseLong(values[0]));
				Text edits = new Text(values[1].substring(0, values[1].indexOf("!@#")));
				
				for (int i = 0; i < values.length; i++)
					System.out.println(">" + values[i] + "<");
					
				System.out.println("values[1]: " + values[1] + "INDEX: " + values[1].indexOf("!@#"));
				
				System.out.println("KEY: " + id + " ||| VALUE: " + edits);
				//System.out.println(values[25]);
				
				output.collect(id, edits);
			}
		}
	}

	public static class Reduce2 extends MapReduceBase implements Reducer<LongWritable, Text, Text, Text> {
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			String value;
			String edits = "";
			String title = "";
			
			while (values.hasNext()) {
				value = values.next().toString();
				
				if (value.startsWith("!@##@!")) {
					// it is an (id,title) pair
					title = value.substring(6);
				} else {
					// it is an (id,edits) pair
					edits += value;
				}
			}
			
			if (edits.equals(""))
				edits = "0";
			
			output.collect(new Text("!@##@!" + title), new Text(edits));
		}
	}
	
		public static class Map3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		/* 
		 * line structure: "[!@##@!]title accesses|edits"
		 */
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] values = line.split("\\s+");
			
			Text title = new Text(values[0]);
			Text outValue;
			
			if (values[0].startsWith("!@##@!")) {
				// it is a (title,edits) pair.
				title = new Text(values[0].substring(6));
				outValue = new Text("!@##@!" + values[1]);
			} else {
				// it is a (title,accesses) pair.
				outValue = new Text(values[1]);
			}

			output.collect(title, outValue);	
		}
	}

	public static class Reduce3 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		/*
		 * output: "page_name ratio edits accesses"
		 */
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			String value;
			Long edits = new Long(0);
			Long accesses = new Long(-1);
			double ratio = 1.0;
			
			while (values.hasNext()) {
				value = values.next().toString();
				
				if (value.startsWith("!@##@!")) {
					// it is a (title,edits) pair.
					edits = Long.parseLong(value.substring(6));
				} else {
					// it is a (title,accesses) pair.
					accesses = Long.parseLong(value);
				}
			}
			
			// collect information only if there is access information for a page.
			if (accesses.longValue() != -1) {
				ratio = (double)(edits.doubleValue() / accesses.doubleValue());
				output.collect(key, new Text(ratio + " " + edits + " " + accesses));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {

		if (args.length < 6) {
			System.err.println("Arguments: titles_sorted out_q3 out_q1 out mappers reducers");
			return;
		}

		// 1) create an output file with (title,id) pairs.
		JobConf conf = new JobConf(EditViewBalance.class);
		conf.setJobName("edit_view_balance");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setNumMapTasks(Integer.parseInt(args[4]));
		// 1 reducer for sorting.
		conf.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[3] + "/1"));

		JobClient.runJob(conf);
		
		// 2) create an output file with (title,edits) pairs
		JobConf conf2 = new JobConf(EditViewBalance.class);
		conf2.setJobName("edit_view_balance2");

		conf2.setOutputKeyClass(LongWritable.class);
		conf2.setOutputValueClass(Text.class);

		conf2.setMapperClass(Map2.class);
		conf2.setReducerClass(Reduce2.class);
		
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);
		
		conf2.setNumMapTasks(Integer.parseInt(args[4]));
		conf2.setNumReduceTasks(Integer.parseInt(args[5]));

		FileInputFormat.setInputPaths(conf2, new Path(args[3] + "/1"), new Path(args[1]));
		FileOutputFormat.setOutputPath(conf2, new Path(args[3] + "/2"));
		
		JobClient.runJob(conf2);
		
		// 3) create the final output file with (title,edit/view ratio)
		JobConf conf3 = new JobConf(EditViewBalance.class);
		conf3.setJobName("edit_view_balance3");

		conf3.setOutputKeyClass(Text.class);
		conf3.setOutputValueClass(Text.class);

		conf3.setMapperClass(Map3.class);
		conf3.setReducerClass(Reduce3.class);
		
		conf3.setInputFormat(TextInputFormat.class);
		conf3.setOutputFormat(TextOutputFormat.class);
		
		conf3.setNumMapTasks(Integer.parseInt(args[4]));
		conf3.setNumReduceTasks(Integer.parseInt(args[5]));

		FileInputFormat.setInputPaths(conf3, new Path(args[3] + "/2"), new Path(args[2]));
		FileOutputFormat.setOutputPath(conf3, new Path(args[3] + "/3"));

		JobClient.runJob(conf3);
	}
}

