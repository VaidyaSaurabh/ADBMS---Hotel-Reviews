package com.neu.edu.project_sentiment_analysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class SentimentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private URI[] files;
	private DoubleWritable result = new DoubleWritable();

	private HashMap<String, String> sentimentMap = new HashMap<String, String>();

	public void setup(Context context) throws IOException

	{
		files = DistributedCache.getCacheFiles(context.getConfiguration());
		System.out.println("files:" + files);
		Path path = new Path(files[0]);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = "";
		while ((line = br.readLine()) != null) {
			String splits[] = line.split(" ");
			sentimentMap.put(splits[0].trim(), splits[1].trim());
		}
		br.close();
		in.close();
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String values[] = value.toString().split(",");
		if (values.length != 0) {
			String hotelName = values[4].toString().trim();
			String review = values[13].toString().trim();
			String[] reviewWords = review.toString().split(" ");
			float sentimentSum = 0;
			try {
				for (String word : reviewWords) {
					if (sentimentMap.containsKey(word)) {
						float num = Float.parseFloat(sentimentMap.get(word));
						sentimentSum += num;
					
					}
				}
				result.set(sentimentSum);
				context.write(new Text(hotelName), result);
			} catch (Exception e) {
				return;
			}

		}
	}
}