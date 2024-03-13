package org.example;

import org.example.datatypes.QuestionWritable;

public class Utils {
	private static final int DEFAULT_INT_VALUE = -1;
	
	public static int tryParseInt(String value) {
		return tryParseInt(value, DEFAULT_INT_VALUE);
	}
	
	public static int tryParseInt(String value, int defaultVal) {
	    try {
	        return Integer.parseInt(value);
	    } catch (NumberFormatException e) {
	        return defaultVal;
	    }
	}
	
	public static long tryParseLong(String value) {
		return tryParseLong(value, (long)DEFAULT_INT_VALUE);
	}
	
	public static long tryParseLong(String value, long defaultVal) {
	    try {
	        return Long.parseLong(value);
	    } catch (NumberFormatException e) {
	        return defaultVal;
	    }
	}
	
	public static QuestionWritable extractQuestionWritable(String value) {
		String line = value.toString();
		
		String[] words = line.split(",");
		
		QuestionWritable question = new QuestionWritable();
		
		try{		
			question.setPostTypeId(Integer.parseInt(words[0]));
			question.setParentId(tryParseInt(words[1]));
			question.setCreationDate(words[2]);
			
			if(words.length > 3){
				question.setAnswerCount(tryParseInt(words[3]));
			}
			
			if(words.length > 4){
				StringBuilder sb = new StringBuilder();
				for(int i=4; i< words.length; i++){
					sb.append(words[i]);
					sb.append(",");
				}
				question.setBody(sb.toString());
			}
			
			return question;
		}catch(Exception e){
			System.out.println("Exception : " + value);
		}
		
		return question;
	}
}
