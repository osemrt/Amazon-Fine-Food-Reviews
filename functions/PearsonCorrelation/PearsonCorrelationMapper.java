package PearsonCorrelation;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class PearsonCorrelationMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, CoupleWritable> {


        public void map(LongWritable key, Text value, OutputCollector <Text, CoupleWritable> output, Reporter reporter) throws IOException {

                String valueString = value.toString();
                String[] fields = valueString.split(",");
                
                if(fields==null || fields.length!=10){
        			return;
                }

                String productId = fields[1];
                int score = Integer.parseInt(fields[6]);
                
                int helpfulnessNumerator = Integer.parseInt(fields[4]);
                int helpfulnessDenominator = Integer.parseInt(fields[5]);

                float helpfulnessRatio;
                if(helpfulnessDenominator!=0) {
                        helpfulnessRatio = (float)helpfulnessNumerator/helpfulnessDenominator;
                }else {
                        helpfulnessRatio = 0.0f;
                }

                output.collect(new Text(productId), new CoupleWritable(new IntWritable(score), new FloatWritable(helpfulnessRatio)));
        }
}
