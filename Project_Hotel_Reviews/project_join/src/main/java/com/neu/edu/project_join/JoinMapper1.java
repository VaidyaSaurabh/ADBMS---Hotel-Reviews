package com.neu.edu.project_join;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class JoinMapper1 extends Mapper<Object, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		
		
		String[] separatedInput = value.toString().split(",");

		String reviewerId = separatedInput[8].trim().replaceAll("\"", "");

		if (reviewerId == null) {
			return;
		}
		outKey.set(reviewerId);
		outValue.set("A" + value);

		
		if(!reviewerId.equals("Question ID")) {
			context.write(outKey, outValue);	
		}
		

	}

}