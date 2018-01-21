package com.neu.edu.project_distinct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Filtering Unique");
		job.setJarByClass(HotelMapper.class);

		job.setMapperClass(HotelMapper.class);
		job.setReducerClass(HotelReducer.class);
		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		FileSystem hdfs = FileSystem.get(conf);

		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
	}

}
