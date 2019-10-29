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
public class EnronDegreeCentralityReducer extends Reducer<Text, Text, Text, Text> {

    private HashMap <String, String> nodeMap = new HashMap<String, String>();
    

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text t : values) {
	   if (!nodeMap.containsKey(t.toString())) nodeMap.put(key.toString(), t.toString());
	   else {
		String currString = nodeMap.get(key.toString());
		currString += " " + t.toString();
		nodeMap.replace(key.toString(), currString);		
	   }
        }	
	
	// Getting an iterator 
        Iterator nmIterator = nodeMap.entrySet().iterator(); 
  	String outputString = new String();
	
	int in_degrees = 0;

        while (nmIterator.hasNext()) { 
            Map.Entry mapElement = (Map.Entry)nmIterator.next(); 
	    String key_str = (String)mapElement.getKey();
	    String value_str = (String)mapElement.getValue();
	    if ( key_str.contains(key.toString()) ) {
		String [] destinations = value_str.split(" ");
            	outputString += Integer.toString(destinations.length) + " ";
	    }
	    else {
		if ( value_str.contains(key.toString()) ) { 
		    in_degrees++;
		}
	    }
        } 	

	outputString += Integer.toString(in_degrees);	
	context.write(key, new Text(outputString));
	
    }

    
}
