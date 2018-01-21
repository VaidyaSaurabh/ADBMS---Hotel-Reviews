package com.neu.edu.project_counters;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CounterMapper extends Mapper<Object, Text, NullWritable, NullWritable> {


	public static final String Country_Group = "Country_Counter";

	private String[] countryArrays = new String[] { "Russia", "Germany", "United Kingdom", "France", "Italy", "Spain",
			"Ukraine", "Poland", "Romania", "Netherlands", "Belgium", "Greece", "Czech Republic", "Portugal", "Sweden",
			"Hungary", "Belarus", "Serbia", "Austria", "Switzerland", "Bulgaria", "Denmark", "Finland", "Slovakia",
			"Norway", "Ireland", "Croatia", "Moldova", "Bosnia & Herzegovina", "Albania", "Lithuania", "TFYR Macedonia",
			"Slovenia", "Latvia", "Estonia", "Montenegro", "Luxembourg", "Malta", "Iceland", "Andorra", "Monaco",
			"Liechtenstein", "San Marino", "Holy See" };

	private HashSet<String> countries = new HashSet<String>(Arrays.asList(countryArrays));

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String fields = value.toString();
		if (fields != null && !fields.toString().isEmpty()) {
			
			for(String country :countries){
				if(fields.contains(country)){
					context.getCounter(Country_Group, country).increment(1);
					break;
				}
			}
		}

	}
}