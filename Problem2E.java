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

public class Problem2E {
	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length < 9 || tokens[2].contains("Unknown")
					|| tokens[4].contains("Unknown")
					|| tokens[4].contains("Before")
					|| !StringUtils.isNumeric(tokens[7])
					|| tokens[6].contains("ARR")
					|| tokens[6].contains("Arr")) {
				return;
			}
			String room = tokens[2];
			String sem = tokens[1];
			String course = tokens[6];
			String year = sem.split(" ")[1];
			
			String room_year;
			try {
				room_year = room+"_"+year;
	
			} catch (Exception e) {
				return;
			}
			
			word.set(room_year);
			
			context.write(word, new Text(course));
		}
	}

	public static class Reducer1 extends
			Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
			
		}
	}

	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			if (tokens.length < 2) {
				return;
			}
			String room_year;
			String course;
			try {
				room_year = tokens[0];
				course = tokens[1];
			} catch (Exception e) {
				return;
			}
			word.set(room_year);
			context.write(word, new Text(course));
		}
	}

	public static class Reducer2 extends
			Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (Text value : values) {
				count++;
			}
			context.write(key, new IntWritable(count));
		}
	}

	public static void main(String[] args) throws Exception {
		String tempOutputFile = "2Etemp";
		Configuration conf = new Configuration();
		new GenericOptionsParser(conf, args);
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: Problem2E <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf,"get  increase or decrease in the number of courses offered with respect to the increase in building and room space over the years");
		job.setJarByClass(Problem2E.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Reducer1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(tempOutputFile));
		job.waitForCompletion(true);

		conf = new Configuration();
		job = Job.getInstance(conf,
				"find the count of the number of courses offered each year");
		job.setJarByClass(Problem2E.class);
		job.setMapperClass(Mapper2.class);
		job.setReducerClass(Reducer2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(tempOutputFile));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
