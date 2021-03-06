package com.neu.edu.project_chaining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		// if (args.length != 2) {
		// System.err.println("Usage: MaxSubmittedCharge <input path> <output
		// path>");
		// System.exit(-1);
		// }
		//

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		Path second_input_path = outputDir;
		Path final_output = new Path(args[2]);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = new Job(conf, "Distinct");
		job.setJarByClass(DistinctMapper.class);

		// Setup MapReduce
		job.setMapperClass(DistinctMapper.class);
		job.setReducerClass(DistinctReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
		if (hdfs.exists(final_output))
			hdfs.delete(final_output, true);

		boolean complete = job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "chaining");

		if (complete) {
			job2.setJarByClass(Driver.class);
			job2.setMapperClass(InvertedIndexMapper.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setReducerClass(InvertedIndexReducer.class);
			job2.setOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);

			FileInputFormat.addInputPath(job2, second_input_path);
			FileOutputFormat.setOutputPath(job2, final_output);

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}

	}

}