package jhu.searchindex;

import java.lang.Integer;
import java.lang.StringBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wilsopw1 on 2/25/17.
 */
public class SIReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {     
	
	Map <String, String> siMap = new HashMap();
	String currValue = key.toString() + " - ";
	
        for(Text v : values) {
	    if (!siMap.containsKey(key.toString())) {
		currValue += v.toString(); 
	 	siMap.put(key.toString(), currValue);
	    }
	    else {
	    	currValue += (" " + v.toString()); 
		siMap.put(key.toString(), currValue); 
	    }
        }

	// Getting an iterator 
        Iterator hmIterator = siMap.entrySet().iterator(); 
	String returnString = new String();
  
        while (hmIterator.hasNext()) { 
            Map.Entry mapElement = (Map.Entry)hmIterator.next(); 
            returnString += ("\t"+mapElement.getKey()+" - "+mapElement.getValue()+"\n");
        } 
	
        context.write(new Text(key), new Text(returnString));
    }
}
