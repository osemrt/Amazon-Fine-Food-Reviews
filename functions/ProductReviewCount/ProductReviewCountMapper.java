package ProductReviewCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class ProductReviewCountMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, OutputCollector <Text, IntWritable> output, Reporter reporter) throws IOException {

                String valueString = value.toString();
                String[] fields = valueString.split(",");
                
                if(fields!=null && fields.length==10){
                	output.collect(new Text(fields[1]), one);
                }
        }
}