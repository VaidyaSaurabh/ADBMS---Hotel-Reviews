package com.neu.edu.project_binning;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class BinningMapper extends Mapper<Object, Text, Text, NullWritable> {
	private MultipleOutputs<Text, NullWritable> mOutputs = null;

	protected void setup(Context context) {

		mOutputs = new MultipleOutputs(context);
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] row = value.toString().split(",");
		String tags = row[9];

		tags = tags.substring(0);
		if (tags.equalsIgnoreCase("Business Trip")) {
			mOutputs.write("bins", value, NullWritable.get(), "Business Trip");
		}
		if (tags.equalsIgnoreCase("Family Outing")) {
			mOutputs.write("bins", value, NullWritable.get(), "Family Outing");
		}
		if (tags.equalsIgnoreCase("Leisure Trip")) {
			mOutputs.write("bins", value, NullWritable.get(), "Leisure Trip");
		}
		if (tags.equalsIgnoreCase("Conference")) {
			mOutputs.write("bins", value, NullWritable.get(), "Conference");
		}
		if (tags.equalsIgnoreCase("Solo Traveller")) {
			mOutputs.write("bins", value, NullWritable.get(), "Solo Traveller");
		}
		if (tags.equalsIgnoreCase("Sponsored Trip")) {
			mOutputs.write("bins", value, NullWritable.get(), "Sponsored Trip");
		}
		if (tags.equalsIgnoreCase("Passing By")) {
			mOutputs.write("bins", value, NullWritable.get(), "Passing By");
		}
		if (tags.equalsIgnoreCase("Couple")) {
			mOutputs.write("bins", value, NullWritable.get(), "Couple");
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		mOutputs.close();
	}

}
