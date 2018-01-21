package com.neu.edu.project_join;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

	private static final Text EMPTY_TEXT = new Text();
	private Text tmp = new Text();

	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();

	private String joinType = null;

	public void setup(Context context) {
		joinType = context.getConfiguration().get("join.type");
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		listA.clear();
		listB.clear();
		while (values.iterator().hasNext()) {
			tmp = values.iterator().next();

			if (tmp.charAt(0) == 'A') {
				// remove appended charachter from the start and add to list
				listA.add(new Text(tmp.toString().substring(1)));

			} else if (tmp.charAt(0) == 'B') {
				// remove appended charachter from the start and add to list
				listB.add(new Text(tmp.toString().substring(1)));
			}
		}

		executeJoinLogic(context);
	}

	private void executeJoinLogic(Context context) throws IOException, InterruptedException {
		if (joinType.equalsIgnoreCase("inner")) {
			if (!listA.isEmpty() && !listB.isEmpty())
				for (Text A : listA) {
					for (Text B : listB) {
						context.write(A, B);

					}
				}
		}
	}
}
