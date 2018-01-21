package com.neu.edu.project_sentiment_analysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SentimentReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	private DoubleWritable result = new DoubleWritable();

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		int sentimentTotal = 0;

		for (DoubleWritable sentiment : values) {
			sentimentTotal += sentiment.get();

		}
		result.set(sentimentTotal);
		context.write(key, result);
	}
}