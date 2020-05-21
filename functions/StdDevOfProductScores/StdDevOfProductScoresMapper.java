package StdDevOfProductScores;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class StdDevOfProductScoresMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, FloatWritable> {


	private FloatWritable score = new FloatWritable();

	public void map(LongWritable key, Text value, OutputCollector <Text, FloatWritable> output, Reporter reporter) throws IOException {
		
		String valueString = value.toString();
		String[] fields = valueString.split(",");
		
		if(fields!=null && fields.length==10){
			String productId = fields[1];
			score.set(Float.parseFloat(fields[6]));
			output.collect(new Text(productId), score);
        }
	}
}
