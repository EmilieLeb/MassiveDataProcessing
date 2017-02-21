package invertedindex;

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

public class InvertedIndex{

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


	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
            	Path pt = new Path("stopwords.csv");
            	FileSystem fs = FileSystem.get(new Configuration());
            	Scanner stopWord = new Scanner(fs.open(pt));
			
			Boolean isStopWord = false;
			while (stopWord.hasNext()){
				if (stopWord.next().toString().equals(key.toString())) {
					isStopWord = true;
					break;
				}
			}
			stopWord.close();
						
			if (!isStopWord){
				
				HashMap<String, Integer> countWord = new HashMap<String, Integer>();
				
				for (Text val : values) {
		            		if(countWord.containsKey(val.toString())){
		               			 countWord.put(val.toString(), countWord.get(val.toString()) + 1);
		            		}else{
		                		countWord.put(val.toString(), 1);
		            		}
		        	}
				
				String fileFrequency = new String();
				for (String fileName: countWord.keySet()){
					String freq = countWord.get(fileName).toString();  
					if (fileFrequency.isEmpty()){
						fileFrequency = fileName + "#" + freq;
					}
					else{
						fileFrequency = fileFrequency + ", "+ fileName + "#" + freq;
					}
				}
				
				Text documentList = new Text();
				documentList.set(fileFrequency.toString());
				context.write(key, documentList);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "InvertedIndex");
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
