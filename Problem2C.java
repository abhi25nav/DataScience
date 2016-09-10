/**
 * Team members:
 * Siddharth Ghodke		sghodke
 * Abhinav Sharma		sharma48
 */


import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Problem2C {
	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length < 9 || tokens[2].contains("Unknown")
					|| tokens[4].contains("Unknown")
					|| tokens[4].contains("Before")
					|| !StringUtils.isNumeric(tokens[7])) {
				return;
			}

			String startTime = tokens[4].split(" ")[0];
			int enrolled = Integer.parseInt(tokens[7]);
			String year;
			try {
				year = tokens[1].split(" ")[1];
			} catch (Exception e) {
				return;
			}

			word.set(startTime + "_" + year);
			context.write(word, new IntWritable(enrolled));
		}
	}

	public static class Reducer1 extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			// int count = 0;
			for (IntWritable value : values) {
				// count++;
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			if (tokens.length < 2) {
				return;
			}
			String year, time;
			int enrolled;
			String period;
			try {
				year = tokens[0].split("_")[1];
				time = tokens[0].split(" ")[0];
				enrolled = Integer.parseInt(tokens[1]);

				if (time.contains("AM")) {
					period = "Morning";
				} else {
					if (time.charAt(0) - '0' <= 4) {
						period = "Afternoon";
					} else {
						period = "Evening";
					}
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				return;
			} catch (NumberFormatException e) {
				return;
			}
			word.set(year + "_" + period);
			context.write(word, new IntWritable(enrolled));
		}
	}

	public static class Reducer2 extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		String tempOutputFile = "2Ctemp";
		Configuration conf = new Configuration();
		new GenericOptionsParser(conf, args);
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: Problem2C <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "get yearly time-wise enrollment");
		job.setJarByClass(Problem2C.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Reducer1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(tempOutputFile));
		job.waitForCompletion(true);

		conf = new Configuration();
		job = Job.getInstance(conf, "get enrollment according to period of the day");
		job.setJarByClass(Problem2C.class);
		job.setMapperClass(Mapper2.class);
		// job.setCombinerClass(Reducer2.class);
		job.setReducerClass(Reducer2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(tempOutputFile));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
