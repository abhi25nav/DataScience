/**
 * Team members:
 * Siddharth Ghodke		sghodke
 * Abhinav Sharma		sharma48
 */


import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Problem2B {
	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length < 9 || tokens[2].contains("Unknown")
					|| !StringUtils.isNumeric(tokens[7])) {
				return;
			}

			String semester = tokens[1];
			int enrolled = Integer.parseInt(tokens[7]);

			word.set(semester);
			context.write(word, new IntWritable(enrolled));
		}
	}

	public static class Reducer1 extends
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

	public static class Mapper2 extends
			Mapper<Object, Text, Text, ObjectWritable> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			if (tokens.length < 2) {
				return;
			}
			String year, sem;
			int enrolled;
			try {
				year = tokens[0].split(" ")[1];
				sem = tokens[0].split(" ")[0];
				// choose only fall and spring sems, ignore winter and summer
				if(!"Fall".equalsIgnoreCase(sem) && !"Spring".equalsIgnoreCase(sem)) {
					return;
				}
				enrolled = Integer.parseInt(tokens[1]);
			} catch (ArrayIndexOutOfBoundsException e) {
				return;
			} catch (NumberFormatException e) {
				return;
			}
			String enr = sem + "_" + enrolled;
			word.set(year);
			context.write(word, new ObjectWritable(enr));
		}
	}

	public static class Reducer2 extends
			Reducer<Text, ObjectWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		public void reduce(Text key, Iterable<ObjectWritable> values,
				Context context) throws IOException, InterruptedException {

			try {
				int fall = 0, spring = 0;
				for(ObjectWritable obj: values) {
					String value = (String) obj.get();
					String[] tokens = value.split("_");
					if("Fall".equalsIgnoreCase(tokens[0])) {
						fall = Integer.parseInt(tokens[1]);
					} else if("Spring".equalsIgnoreCase(tokens[0])) {
						spring = Integer.parseInt(tokens[1]);
					}
				}
				double ratio;
				if(spring == 0 || fall == 0) {
					ratio = 0;
				} else {
					ratio = (double)spring/fall;
				}
				result.set(ratio);
			} catch (Exception e) {
				result.set(0);
			}
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		String tempOutputFile = "2Btemp";
		Configuration conf = new Configuration();
		new GenericOptionsParser(conf, args);
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: Problem2B <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "get semester-wise enrollment");
		job.setJarByClass(Problem2B.class);
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
		job = Job.getInstance(conf, "yearly ratio between spring and fall");
		job.setJarByClass(Problem2B.class);
		job.setMapperClass(Mapper2.class);
		//job.setCombinerClass(Reducer2.class);
		job.setReducerClass(Reducer2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ObjectWritable.class);
		FileInputFormat.addInputPath(job, new Path(tempOutputFile));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
