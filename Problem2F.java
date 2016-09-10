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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Problem2F {
	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length < 9 || tokens[2].contains("Unknown")
					|| tokens[4].contains("Unknown")
					|| tokens[4].contains("Before")
					|| !StringUtils.isNumeric(tokens[7]) 
					|| !StringUtils.isNumeric(tokens[8])) {
				return;
			}
			String time = tokens[4];
			int enrolled = Integer.parseInt(tokens[7]);
			int capacity = Integer.parseInt(tokens[8]);
			if (enrolled == 0 || capacity == 0) {
				return;
			}
			String enrolled_capacity = enrolled + "_" + capacity;
			String class_time;
			try {
				class_time = tokens[6] + "_" + time;
			} catch (Exception e) {
				return;
			}
			
			word.set(class_time);
			context.write(word, new Text(enrolled_capacity));
		}
	}

	public static class Reducer1 extends
			Reducer<Text, Text, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		private Text word = new Text();
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int enrolled = 0;
			int capacity = 0;
			double ratio = 0;
			for (Text value : values) {
				enrolled = Integer.parseInt(value.toString().split("_")[0]); 
				capacity = Integer.parseInt(value.toString().split("_")[1]);
				String key_capacity = key + "_" + capacity;
				word.set(key_capacity);
				ratio = (double)enrolled / capacity;		
				result.set(ratio);
				context.write(word, result);
			}
		}
	}

	public static class Mapper2 extends Mapper<Object, Text, DoubleWritable, Text> {

		private DoubleWritable word = new DoubleWritable();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			if (tokens.length < 2) {
				return;
			}
			String key_capacity;
			double ratio;
			String class_time;
			try {
				key_capacity = tokens[0];
				ratio = Double.parseDouble(tokens[1]);
				String course = key_capacity.split("_")[0];
				String time = key_capacity.split("_")[1];
				int capacity = Integer.parseInt(key_capacity.split("_")[2]);
				if(capacity < 150){
					return;
				}
				class_time = course + "_" + time;
			} catch (NumberFormatException e) {
				return;
			}
			word.set(ratio);
			context.write(word, new Text(class_time));
		}
	}

	public static class Reducer2 extends
			Reducer<DoubleWritable, Text, DoubleWritable, Text> {

		public void reduce(DoubleWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		String tempOutputFile = "2Ftemp";
		Configuration conf = new Configuration();
		new GenericOptionsParser(conf, args);
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: Problem2F <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf,"get ratio of enrolment/capacity of courses based on the time at which they are offered");
		job.setJarByClass(Problem2F.class);
		job.setMapperClass(Mapper1.class);
		//job.setCombinerClass(Reducer1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(tempOutputFile));
		job.waitForCompletion(true);

		conf = new Configuration();
		job = Job.getInstance(conf,"sort the courses based on ratio of enrolment/capacity for capacity greater than 150");
		job.setJarByClass(Problem2F.class);
		job.setMapperClass(Mapper2.class);
		job.setReducerClass(Reducer2.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(tempOutputFile));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

