package com.neu.edu.project_distinct;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String values[] = value.toString().split(",");

		Float reviewScore = 0.0f;
		String hotelName = "";
		String range = "";
		try {

			hotelName = values[4];
			reviewScore = Float.parseFloat(values[3]);

		} catch (Exception e) {
		}
		try {

			Integer tmp = 0;
			tmp = (int) (reviewScore / 100);
			range = (tmp * 10 + "-" + (tmp + 1));
			context.write(new Text(range), new Text(hotelName));
		} catch (Exception e) {

			System.out.println("" + e.getMessage());

		}
	}
}