package com.neu.edu.project_counters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// String[] otherArgs = new GenericOptionsParser(conf, args)
		// .getRemainingArgs();

		if (args.length != 2) {
			System.err.println("Usage: CountNumUsersByState <users> <out>");
			System.exit(2);
		}
		
		conf.setInt(MRJobConfig.COUNTERS_MAX_KEY, 7000);

		Path input = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Job job = new Job(conf, "Counting of Country");
		job.setJarByClass(CounterMapper.class);

		job.setMapperClass(CounterMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputDir);
		int code = job.waitForCompletion(true) ? 0 : 1;
		
		// Delete output if exists
				FileSystem hdfs = FileSystem.get(conf);
				if (hdfs.exists(outputDir))
					hdfs.delete(outputDir, true);

		if (code == 0) {
			for (Counter counter : job.getCounters().getGroup(CounterMapper.Country_Group)) {
				System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
			}
		}

		System.exit(code);
	}

}