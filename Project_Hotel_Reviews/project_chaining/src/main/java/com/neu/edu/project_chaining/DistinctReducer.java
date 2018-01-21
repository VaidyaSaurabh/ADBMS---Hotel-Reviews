package com.neu.edu.project_chaining;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistinctReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	public void reduce(Text key, Iterable<FloatWritable> value, Context context)
			throws IOException, InterruptedException {
		context.write(key, value.iterator().next());

	}
}