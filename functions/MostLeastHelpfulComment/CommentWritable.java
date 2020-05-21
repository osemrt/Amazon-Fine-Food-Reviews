package MostLeastHelpfulComment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CommentWritable implements Writable{
	
	private Text userId;
	private FloatWritable helpfulnessRatio;
	private Text text;
	

	public CommentWritable() {
		
	}

	public CommentWritable(Text userId, FloatWritable helpfulnessRatio, Text text) {
		this.userId = userId;
		this.helpfulnessRatio = helpfulnessRatio;
		this.text = text;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		userId = new Text(in.readUTF());
		helpfulnessRatio = new FloatWritable(Float.parseFloat(in.readUTF()));
		text = new Text(in.readUTF());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(userId.toString());	
		out.writeUTF(helpfulnessRatio.toString());
		out.writeUTF(text.toString());
	}

	public Text getUserId() {
		return userId;
	}

	public void setUserId(Text userId) {
		this.userId = userId;
	}

	public FloatWritable getHelpfulnessRatio() {
		return helpfulnessRatio;
	}

	public void setHelpfulnessRatio(FloatWritable helpfulnessRatio) {
		this.helpfulnessRatio = helpfulnessRatio;
	}

	public Text getText() {
		return text;
	}

	public void setText(Text text) {
		this.text = text;
	}

	@Override
	public String toString() {
		return "[userId=" + userId + ", helpfulnessRatio=" + helpfulnessRatio + ", text=" + text + "]";
	}
	
}
