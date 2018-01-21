package com.neu.edu.project_join;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper2 extends Mapper<Object, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String reviewerId = value.toString().split(",")[1].trim();
		if (reviewerId == null) {
			return;
		}
		outKey.set(reviewerId);
		outValue.set("B" + ","+value.toString().split(",")[0].trim());
		
		
		if(!reviewerId.equals("Question ID")) {
			context.write(outKey, outValue);	
		}
	}
}