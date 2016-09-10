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

public class Problem2A {
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

			String courseName = tokens[6];
			int enrolled = Integer.parseInt(tokens[7]);
			if (enrolled == 0) {
				return;
			}
			String year;
			try {
				year = tokens[1].split(" ")[1];
				if (Integer.parseInt(year) < 2006
						|| Integer.parseInt(year) >= 2016) {
					return;
				}
				String semester = tokens[1].split(" ")[0];
				if (!"Fall".equalsIgnoreCase(semester)) {
					return;
				}
			} catch (Exception e) {
				return;
			}

			word.set(courseName);
			context.write(word, new IntWritable(enrolled));
		}
	}

	public static class Reducer1 extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			for (IntWritable value : values) {
				count++;
				sum += value.get();
			}
			result.set(sum / count);
			context.write(key, result);
		}
	}

	public static class Mapper2 extends Mapper<Object, Text, IntWritable, Text> {

		private IntWritable word = new IntWritable();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			if (tokens.length < 2) {
				return;
			}
			String courseName;
			int enrolled;
			try {
				courseName = tokens[0];
				enrolled = Integer.parseInt(tokens[1]);
			} catch (NumberFormatException e) {
				return;
			}
			word.set(enrolled);
			context.write(word, new Text(courseName));
		}
	}

	public static class Reducer2 extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		String tempOutputFile = "2Atemp";
		Configuration conf = new Configuration();
		new GenericOptionsParser(conf, args);
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: Problem2A <in> <out>");
			System.exit(2);
		}
		Job job = Job
				.getInstance(conf,
						"get average enrollment over last 10 years for all the courses in Fall");
		job.setJarByClass(Problem2A.class);
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
		job = Job.getInstance(conf,
				"sort the courses based on average enrollment");
		job.setJarByClass(Problem2A.class);
		job.setMapperClass(Mapper2.class);
		// job.setCombinerClass(Reducer2.class);
		job.setReducerClass(Reducer2.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(tempOutputFile));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
