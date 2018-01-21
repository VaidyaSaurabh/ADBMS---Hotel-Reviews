package com.neu.edu.project_partition;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartitionMapper extends	Mapper<Object, Text, IntWritable, Text> {

	private IntWritable outkey = new IntWritable();

	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		try{
		String strDate=value.toString().split(",")[2];

		int year = Integer.parseInt(strDate.split("/")[2]);
		outkey.set(year);

		context.write(outkey, value);
		}catch(Exception e){
			return;
		}
	}
}