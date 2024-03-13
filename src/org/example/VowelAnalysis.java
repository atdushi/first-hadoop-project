package org.example;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class VowelAnalysis {
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			//System.out.println("MAP CALLED VALUE IS: " + line);
			String body = line.toLowerCase();
			char[] words = body.toCharArray();
			String vowels = "aieou";

			for (char word : words) {
				if (vowels.indexOf(word) > -1) {
					context.write(new Text(String.valueOf(word)), new LongWritable(1));
				}
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		Configuration conf = new Configuration();
		
		//conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		Job job = Job.getInstance(conf);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(MapClass.class);

		job.setReducerClass(LongSumReducer.class);

		//job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJarByClass(WordAnalysis.class);
		job.submit();

	}
}
