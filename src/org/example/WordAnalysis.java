package org.example;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class WordAnalysis {
	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			System.out.println("MAP CALLED VALUE IS: " + line);
			String body = line.replaceAll("[^A-Za-z\\s]", "").toLowerCase();
			String[] words = body.split(" ");

			for (String word : words) {
				if (word.length() >= 7) {
					context.write(new Text(word), new LongWritable(1));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		public void reduce(Text key, Iterable<LongWritable> values,	Context context) throws IOException, InterruptedException {
			Long sum = 0l;
			Iterator<LongWritable> it = values.iterator();
			while (it.hasNext()) {
				sum += it.next().get();
			}
			context.write(key, new LongWritable(sum));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(MapClass.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJarByClass(WordAnalysis.class);
		job.submit();

	}
}
