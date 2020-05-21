package MostLeastHelpfulComment;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class MostLeastHelpfulMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, CommentWritable> {


        public void map(LongWritable key, Text value, OutputCollector <Text, CommentWritable> output, Reporter reporter) throws IOException {

                String valueString = value.toString();
                String[] fields = valueString.split(",");
                
                if(fields==null || fields.length!=10){
        			return;
                }

                String productId = fields[1];
                String userId = fields[2];
                
                int helpfulnessNumerator;
                int helpfulnessDenominator;
                try
                {
                	helpfulnessNumerator = Integer.parseInt(fields[4]);
                    helpfulnessDenominator = Integer.parseInt(fields[5]);
                }
                catch(NumberFormatException nfe)
                {
                    return;
                }
                
                
                
                String comment = fields[9];

                float helpfulnessRatio;
                if(helpfulnessDenominator!=0) {
                	helpfulnessRatio = (float)helpfulnessNumerator/helpfulnessDenominator;
                }else {
                	helpfulnessRatio = 0;
                }
                
                output.collect(new Text(productId), new CommentWritable(new Text(userId), new FloatWritable(helpfulnessRatio), new Text(comment)));
        }
}
