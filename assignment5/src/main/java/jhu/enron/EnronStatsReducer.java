package jhu.enron;

import java.util.*;
import java.lang.Integer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wilsopw1 on 2/25/17.
 */
public class EnronStatsReducer extends Reducer<Text, Text, Text, Text> {

    

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    
    	int toFreq = 0;
    	int ccFreq = 0; 

	String stringVal = new String();
	String outputString = new String();

	String [] nodes = key.toString().split(" ");

        for(Text t : values) {
	   stringVal = t.toString();
	   System.out.println(stringVal);
	   if (stringVal.equals("To")) toFreq++;
	   if (stringVal.equals("Cc")) ccFreq++;
        }	
	outputString = 	  nodes[1]; 
		
	context.write(new Text(nodes[0]), new Text(outputString));
	
    }
}
