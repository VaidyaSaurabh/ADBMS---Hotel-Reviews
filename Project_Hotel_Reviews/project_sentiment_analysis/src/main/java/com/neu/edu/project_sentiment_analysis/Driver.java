package com.neu.edu.project_sentiment_analysis;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class Driver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();


		Path final_output = new Path(args[2]);
		Job job = new Job(conf, "SentimentAnalysis");

		job.setJarByClass(Driver.class);

		job.setMapperClass(SentimentMapper.class);

		job.setReducerClass(SentimentReducer.class);

		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, final_output);
		DistributedCache.addCacheFile(new URI(args[1]), job.getConfiguration());

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(final_output))
			hdfs.delete(final_output, true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}