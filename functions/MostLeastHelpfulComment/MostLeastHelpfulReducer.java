package MostLeastHelpfulComment;

import java.io.IOException;

import java.util.*;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class MostLeastHelpfulReducer extends MapReduceBase implements Reducer<Text, CommentWritable, Text, Text> {

		private CommentWritable mostHelpfulComment;
		private CommentWritable leastHelpfulComment;

        public void reduce(Text t_key, Iterator<CommentWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        		float maxRatio = Float.MIN_VALUE;
        		float minRatio = Float.MAX_VALUE;
        		
        		while(values.hasNext()) {
        			CommentWritable value = values.next();
        			float helpfulnessRatio = value.getHelpfulnessRatio().get();
        			if(helpfulnessRatio > maxRatio) {
        				maxRatio = helpfulnessRatio;
        				mostHelpfulComment = value;
        			}
        			
        			if(helpfulnessRatio < minRatio) {
        				minRatio = helpfulnessRatio;
        				leastHelpfulComment = value;
        			}
        		}


			output.collect(t_key, new Text("mostHelpful_comment: " + mostHelpfulComment.toString() + " | " + "leastHelpful_comment: " + leastHelpfulComment.toString()));

        }
}
