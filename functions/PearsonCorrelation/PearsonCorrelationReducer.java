package PearsonCorrelation;

import java.io.IOException;

import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class PearsonCorrelationReducer extends MapReduceBase implements Reducer<Text, CoupleWritable, Text, Text> {

        public void reduce(Text t_key, Iterator<CoupleWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        				int count = 0;
        				float sum_X = 0, sum_Y = 0, sum_XY = 0; 
        				float squareSum_X = 0, squareSum_Y = 0;

                        while(values.hasNext()) {
                        	CoupleWritable value = values.next(); 
                        	int score = value.getScore().get();
                        	float helpfulnessRatio = value.getHelpfulnessRatio().get();
                        	
                        	sum_X +=  score;
                        	sum_Y += helpfulnessRatio;
                        	
                        	sum_XY += score * helpfulnessRatio;
                        	
                        	squareSum_X += score * score;
                        	squareSum_Y += helpfulnessRatio * helpfulnessRatio;
                        	
                        	count += 1;
                                
                        }
                        
                        float numerator = (float)(count * sum_XY - sum_X * sum_Y);
                        float denominator = (float) Math.sqrt((count * squareSum_X - sum_X * sum_X)  * (count * squareSum_Y - sum_Y * sum_Y));
                        
                        float correlation = 0;
                        if(denominator!=0) {
                        	correlation = numerator / denominator;
                        }

                output.collect(t_key, new Text(Float.toString(correlation)));
        }
}
