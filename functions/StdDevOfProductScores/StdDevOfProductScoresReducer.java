package StdDevOfProductScores;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class StdDevOfProductScoresReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, Text> {

	private FloatWritable result = new FloatWritable();

	public void reduce(Text t_key, Iterator<FloatWritable> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {				
		Text key = t_key;
		int count = 0;
		int sum = 0;

		ArrayList<FloatWritable> cache = new ArrayList<FloatWritable>();
		while(values.hasNext()){
			FloatWritable value = values.next();
			sum += value.get();
			count += 1;
			cache.add(value);
		}

		float sumOfSquares = 0;
		float mean = 0;
		if(count!=0) {
			mean = (float)sum/count;
		}
		
		for(FloatWritable value : cache){
			float score = value.get();
			sumOfSquares += (score - mean) * (score - mean);
		}
		
		result.set((float) Math.sqrt(sumOfSquares/(count)));
		output.collect(key, new Text(String.format("%.2f", result.get())));
	}
}
