package com.neu.edu.project_chaining;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistinctMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split(",");
		try{
			float k1 = Float.parseFloat(fields[3]);
		

		context.write(new Text(fields[4]), new FloatWritable(k1));
		} catch (Exception e){
			return;
		}
	}
}