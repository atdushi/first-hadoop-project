package org.example.case1;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.example.Utils;
import org.example.datatypes.QuestionWritable;



public class QuestionsWithNoResponses {

	public static class MyMapClass extends Mapper<Text, Text, LongWritable, LongWritable> {

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			
			//System.out.println("MAP CALLED VALUE IS: " + line);
			
			QuestionWritable question = Utils.extractQuestionWritable(line);
			
			//System.out.println(question.toString());
			
			long k = Utils.tryParseLong(key.toString());
			
			if(k > 0){
				context.write(new LongWritable(k), new LongWritable(question.getAnswerCount() > 0 ? 1 : 0));
			}
		}
	}

	public static class MyReduceClass extends Reducer<LongWritable, LongWritable, Text, LongWritable> {
		
		public void reduce(LongWritable key, Iterable<LongWritable> values,	Context context) throws IOException, InterruptedException {
			
			Iterator<LongWritable> it = values.iterator();
			
			Long sum = 0l;
			
			while (it.hasNext()) {
				sum += it.next().get();
			}
			context.write(new Text(key.toString()), new LongWritable(sum));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		deleteDirectory(new File("/home/node1/workspace/FirstHadoopProject/output"));
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		Job job = Job.getInstance(conf);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(MyMapClass.class);
//		job.setCombinerClass(MyReduceClass.class);
		job.setReducerClass(MyReduceClass.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJarByClass(QuestionsWithNoResponses.class);
		job.submit();

	}
	
	static boolean deleteDirectory(File directoryToBeDeleted) {
	    File[] allContents = directoryToBeDeleted.listFiles();
	    if (allContents != null) {
	        for (File file : allContents) {
	            deleteDirectory(file);
	        }
	    }
	    return directoryToBeDeleted.delete();
	}

}
