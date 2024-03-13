package org.example.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class QuestionWritable implements Writable{
	int postTypeId;
	long parentId;
	String creationDate;
	int answerCount;
	String body;

	public QuestionWritable() {
	}
	
	public QuestionWritable(int postTypeId, 
			long parentId,
			String creationDate, 
			int answerCount, 
			String body) {
		super();
		this.postTypeId = postTypeId;
		this.parentId = parentId;
		this.creationDate = creationDate;
		this.answerCount = answerCount;
		this.body = body;
	}

	@Override
	public String toString() {
		return "QuestionWritable [postTypeId=" + postTypeId + ", parentId="
				+ parentId + ", creationDate=" + creationDate
				+ ", answerCount=" + answerCount + ", body=" + body + "]";
	}

	public long getPostTypeId() {
		return postTypeId;
	}

	public void setPostTypeId(int postTypeId) {
		this.postTypeId = postTypeId;
	}

	public long getParentId() {
		return parentId;
	}

	public void setParentId(long parentId) {
		this.parentId = parentId;
	}

	public String getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(String creationDate) {
		this.creationDate = creationDate;
	}

	public int getAnswerCount() {
		return answerCount;
	}

	public void setAnswerCount(int answerCount) {
		this.answerCount = answerCount;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		postTypeId = arg0.readInt();
		parentId = arg0.readLong();
		creationDate = arg0.readUTF();
		answerCount = arg0.readInt();
		body = arg0.readUTF();				
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		 arg0.writeInt(postTypeId);
		 arg0.writeLong(parentId);
		 arg0.writeUTF(creationDate);
		 arg0.writeInt(answerCount);
		 arg0.writeUTF(body);
	}
}
