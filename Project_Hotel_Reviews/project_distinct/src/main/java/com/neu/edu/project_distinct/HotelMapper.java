package com.neu.edu.project_distinct;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HotelMapper extends Mapper<Object, Text, Text, NullWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split(",");

		context.write(new Text(fields[4]+"-"+fields[0]), NullWritable.get());

	}
}