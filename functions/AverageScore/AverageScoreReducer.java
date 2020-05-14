package AverageScore;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class AverageScoreReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, Text> {

	private FloatWritable result = new FloatWritable();

	public void reduce(Text t_key, Iterator<FloatWritable> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
		Text key = t_key;
		int count = 0;
		float sum = 0;
		while (values.hasNext()) {
			FloatWritable value = (FloatWritable) values.next();
			sum += value.get();
			count += 1;
			
		}
		
		result.set((float)sum/count);

		output.collect(key, new Text(String.format("%.2f", result.get())));
	}
}
