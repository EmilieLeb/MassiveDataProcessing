import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class WordCountpro {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "WordCountjob");
		job.setJarByClass(WordCountpro.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(IntSumReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if ((args.length >= 5) && (Integer.parseInt(args[4]) == 1)) {
			// For compression
			conf.set("mapreduce.map.output.compress","true");
		}
		
		// For reducers and compilers
		if (args.length >= 3) {
			job.setNumReduceTasks(Integer.parseInt(args[2]));
		} 
		if ((args.length >=4) && (Integer.parseInt(args[3]) == 1)) {
			job.setCombinerClass(IntSumReducer.class);
		}

		job.waitForCompletion(true);
		
	}

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().toLowerCase().trim();
			StringTokenizer tokenizer = new StringTokenizer(line, " &'(-_)=+$£*!%§:/;.,?<>\"");
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable(10);
	
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
				int sum = 0;
				for (IntWritable val : values) {
					sum += val.get();
				}
			if (sum > 4000) {
				result.set(sum);
				context.write(key, new IntWritable(sum));
			}
		}
	}
}


