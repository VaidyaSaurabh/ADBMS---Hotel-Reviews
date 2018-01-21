package com.neu.edu.project_chaining;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		StringBuilder result = new StringBuilder();
		for (Text value : values) {
			result.append("\n\t\t"+value);
		}
		context.write(key, new Text(result.toString()));
	}

}
