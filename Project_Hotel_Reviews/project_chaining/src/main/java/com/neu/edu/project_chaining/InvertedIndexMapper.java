package com.neu.edu.project_chaining;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String values[] = value.toString().split("\t");

		Float reviewScore = 0.0f;
		String hotelName = "";
		String range = "";
		try {

			hotelName = values[0];
			reviewScore = Float.parseFloat(values[1]);
			int tmp = 0;
			tmp = reviewScore.intValue();
			range = (tmp + "-" + (tmp + 1));
			context.write(new Text(range), new Text(hotelName));
		} catch (Exception e) {
			return;
		}
	}
}
