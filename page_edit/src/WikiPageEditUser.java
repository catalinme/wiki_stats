package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WikiPageEditUser {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		/* 
		 * line structure: "revision_id page_id text_id comment user_id user_name timestamp revision_minor 0 NULL NULL"
		 */
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] values = line.split("\\s+");
			Text user = new Text(values[values.length - 6]);
			String comment = "";

			for (int i = 3; i < values.length - 7; i++)
				comment += values[i];

			System.out.println("LINE: " + line);
			System.out.println("USER: " + user);
			System.out.println("COMMENT: " + comment);

			// send timestamp, page_id, comment
			output.collect(user, new Text(values[values.length - 5] + "!@#" + values[1] + "!@#" + comment + "!@#"));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			String value;
			System.out.println("KEY: " + key);
			int edits = 0;
			String[] splitValues;
			String timestamps = "";
			String pageIds = "";
			String actions = "";
			String lowerCase = "";
			
			while (values.hasNext()) {
				value = values.next().toString();
				splitValues = value.split("!@#");

				edits++;

				if (splitValues.length > 0)
					timestamps += splitValues[0] + " ";
					
				if (splitValues.length > 1)					
					pageIds += splitValues[1] + " ";

				if (splitValues.length > 2) {
					lowerCase = splitValues[2].toLowerCase();
					actions = getActions(lowerCase);
				}
			}
			
			if (timestamps.equals(""))
				timestamps = " ";
				
			if (pageIds.equals(""))
				pageIds = " ";
				
			if (actions.equals(""))
				actions = " ";
			
			output.collect(key, new Text("" + edits +
				"!@#" + timestamps.substring(0, timestamps.length() - 1) +
				"!@#" + pageIds.substring(0, pageIds.length() - 1) +
				"!@#" + actions.substring(0, actions.length() - 1)));
		}
	}
	
	public static class ReduceEdits extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String value;
			System.out.println("KEY: " + key);
			int edits = 0;
			String[] splitValues;
			String timestamps = "";
			String pageIds = "";
			String actions = "";
			String lowerCase = "";
			
			while (values.hasNext()) {
				value = values.next().toString();
				System.out.println("VALUE: " + value);

				splitValues = value.split("!@#");

				edits++;
				
				if (splitValues.length > 0)
					timestamps += splitValues[0] + " ";
					
				if (splitValues.length > 1)
					pageIds += splitValues[1] + " ";

				if (splitValues.length > 2) {
					lowerCase = splitValues[2].toLowerCase();
					actions = getActions(lowerCase);
				}
			}
			
			if (timestamps.equals(""))
				timestamps = " ";
				
			if (pageIds.equals(""))
				pageIds = " ";
				
			if (actions.equals(""))
				actions = " ";

			output.collect(new Text("" + edits), new Text(key.toString() +
				"!@#" + timestamps.substring(0, timestamps.length() - 1) +
				"!@#" + pageIds.substring(0, pageIds.length() - 1) +
				"!@#" + actions.substring(0, actions.length() - 1)));
		}
	}
	
	public static class MapSort extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] values = line.split("\\s+");
			int index = line.indexOf(values[1]);

			System.out.println("LINE " + line);
			System.out.println("KEY " + values[0]);
			System.out.println("VALUE " + line.substring(index));
			
			output.collect(new LongWritable(Long.parseLong(values[0])), new Text(line.substring(index)));
		}
	}

	public static class ReduceSort extends MapReduceBase implements Reducer<LongWritable, Text, Text, Text> {
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String value;
			System.out.println("KEY: " + key);
			int edits = 0;
			String[] splitValues;
			String timestamps = "";
			String users = "";
			String actions = "";
			
			while (values.hasNext()) {
				value = values.next().toString();

				System.out.println("VALUE: " + value);

				splitValues = value.split("!@#");

				for (String splitValue : splitValues)
					System.out.println(">" + splitValue + "<");

				output.collect(new Text(splitValues[0]),
					new Text("" + key + value.substring(splitValues[0].length())));
			}
		}
	}
	
	public static String getActions(String comment) {
		String[] possibleActions = {"revert", "edit", "fix", "add", "tag", "insert", "remov", "updat", "mov", "creat"};
		String out = "";

		for (String action : possibleActions) {
			if (comment.contains(action))
				out += action + " ";
		}

		if (out.equals(""))
			return " ";

		return out;
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 5) {
			System.err.println("Arguments: in out mappers reducers sort");
			return;
		}

		JobConf conf = new JobConf(WikiPageEditUser.class);
		conf.setJobName("page_edit_user");

		conf.setOutputKeyClass(Text.class);
		if (args[4].equals("id") || args[4].equals("no"))
			conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		if (args[4].equals("id") || args[4].equals("no"))
			conf.setReducerClass(Reduce.class);
		else
			conf.setReducerClass(ReduceEdits.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setNumMapTasks(Integer.parseInt(args[2]));
		conf.setNumReduceTasks(Integer.parseInt(args[3]));

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		if (args[4].equals("id") || args[4].equals("no"))
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		else
			FileOutputFormat.setOutputPath(conf, new Path(args[1] + "/1"));

		JobClient.runJob(conf);
		
		if (args[4].equals("id") || args[4].equals("no"))
			return;

		// run second MapReduce job for sorting by number of edits.
		JobConf secondConf = new JobConf(WikiPageEditUser.class);
		secondConf.setJobName("page_edit_user2");

		secondConf.setOutputKeyClass(LongWritable.class);
		secondConf.setOutputValueClass(Text.class);

		secondConf.setMapperClass(MapSort.class);
		secondConf.setReducerClass(ReduceSort.class);

		secondConf.setInputFormat(TextInputFormat.class);
		secondConf.setOutputFormat(TextOutputFormat.class);

		// use just one Reducer in order to get a sorted file.
		secondConf.setNumMapTasks(Integer.parseInt(args[2]));
		secondConf.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(secondConf, new Path(args[1] + "/1"));
		FileOutputFormat.setOutputPath(secondConf, new Path(args[1] + "/2"));

		JobClient.runJob(secondConf);
	}
}

