import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InvertedIndex {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			FileSplit split = (FileSplit) context.getInputSplit();
			String filename = split.getPath().getName().toString();

			StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase(), " &'(-_)=+$£*!%§:/;.,?<>\"");
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, new Text(filename));
			}
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Content content) throws IOException, InterruptedException {
		
		if(!stopWords.contains(key.toString())){
			String filesFrequency = new String();
			for (Text val : values) {
				if(!filesFrequency.contains(val.toString())){
					filesFrequency = filesFrequency + val.toString() + ", ";
				}
			}
			filesFrequency = filesFrequency.substring(0,filesFrequency.length()-2);
			Text finalIndex = new Text();
			finalIndex.set(filesFrequency);
			context.write(key, finalIndex);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Inverted Index");
		job.setJarByClass(InvertedIndex.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
