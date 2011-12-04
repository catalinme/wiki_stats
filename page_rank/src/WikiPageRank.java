package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WikiPageRank {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		/* 
		 * line structure: "from1: to11 to12 to13..."
		 */
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] values = line.split("\\s+");
			Text from = new Text();
			String[] links = new String[0];
			from.set(values[0].substring(0, values[0].length() - 1));

			System.out.println("LINE: " + line);
			System.out.println("FROM: " + from);

			if (values.length > 1) {
				if (!values[1].startsWith("r")) {
					// first run
					int totalLinks = values.length - 1;
					links = new String[totalLinks];

					for (int i = 1; i < values.length; i++) {
						Text out = new Text("1.0 " + totalLinks);
						output.collect(new Text(values[i] + ":"), out);
						System.out.println("!" + values[i] + ": " + out);
						links[i - 1] = values[i];
					}
				} else {
					// next runs
					int totalLinks = values.length - 2;
					links = new String[totalLinks];

					for (int i = 2; i < values.length; i++) {
						Text out = new Text(values[1].substring(1) + " " + totalLinks);
						output.collect(new Text(values[i] + ":"), out);
						System.out.println("@" + values[i] + ": " + out);
						links[i - 2] = values[i];
					}
				}
			}

			String outLinks = "";

			for (String link : links)
				outLinks += link + " ";
				
			output.collect(new Text(values[0]), new Text("#" + outLinks));
			System.out.println(">" + new Text(values[0]) + " " + new Text("#" + outLinks));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			String[] splitValues;
			float otherRank = 0;
			String value;
			String links = "";
			float pageRank;
			int linksCount;
			float sum = 0;

			System.out.println("KEY: " + key);
			
			while (values.hasNext()) {
				value = values.next().toString();

				System.out.println(">>" + value + "<<");

				if (value.startsWith("#")) {
					links = " " + value.substring(1);
					continue;
				}

				splitValues = value.split("\\s+");
				pageRank = Float.valueOf(splitValues[0]);
				linksCount = Integer.valueOf(splitValues[1]);

				sum += (pageRank / (float)linksCount);
			}

			pageRank = 0.85f * sum + 0.15f;
			output.collect(key, new Text("r" + pageRank + " " + links));
		}
	}

	private static void computeRank(Path inPath, Path outPath) throws Exception {
		JobConf conf = new JobConf(WikiPageRank.class);
		conf.setJobName("pagerank");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, inPath);
		FileOutputFormat.setOutputPath(conf, outPath);

		JobClient.runJob(conf);

	}

	public static void main(String[] args) throws Exception {

		int runs = 0;

		if (args.length < 3) {
			System.err.println("Arguments: in out runs");
			return;
		}

		runs = Integer.parseInt(args[2]);

		if (runs < 1) {
			System.err.println("Too few runs.");
			return;
		}

		if (runs == 1) {
			computeRank(new Path(args[0]), new Path(args[1]));
		} else {
			computeRank(new Path(args[0]), new Path(args[1] + "/1"));

			for (int i = 1; i < runs - 1; i++) {
				computeRank(new Path(args[1] + "/" + i), new Path(args[1] + "/" + (i + 1)));
			}

			computeRank(new Path(args[1] + "/" + (runs - 1)), new Path(args[1] + "/" + runs));
		}
	}
}

