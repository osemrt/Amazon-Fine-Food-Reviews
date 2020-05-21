package PearsonCorrelation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class CoupleWritable implements Writable{
	
	private IntWritable score;
	private FloatWritable helpfulnessRatio;
	
	public CoupleWritable() {
		
	}

	public CoupleWritable(IntWritable score, FloatWritable helpfulnessRatio) {
		this.score = score;
		this.helpfulnessRatio = helpfulnessRatio;
	}

	
	@Override
	public void readFields(DataInput in) throws IOException {
		score = new IntWritable(Integer.parseInt(in.readUTF()));
		helpfulnessRatio = new FloatWritable(Float.parseFloat(in.readUTF()));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(score.toString());
		out.writeUTF(helpfulnessRatio.toString());		
	}
	
	
	public IntWritable getScore() {
		return score;
	}

	public void setScore(IntWritable score) {
		this.score = score;
	}

	public FloatWritable getHelpfulnessRatio() {
		return helpfulnessRatio;
	}

	public void setHelpfulnessRatio(FloatWritable helpfulnessRatio) {
		this.helpfulnessRatio = helpfulnessRatio;
	}

	@Override
	public String toString() {
		return "[score=" + score + ", helpfulnessRatio=" + helpfulnessRatio + "]";
	}

}
