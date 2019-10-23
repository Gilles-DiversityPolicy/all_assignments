package jhu.searchindex;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.lang.Integer;
import java.lang.StringBuilder;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.List;
import java.util.ArrayList;

import java.io.IOException;

/**
 * Created by wilsopw1 on 2/25/17.
 */
public class SIMapper extends Mapper<LongWritable, Text, Text, Text> {

    String inputPath = null;
    private int i = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // read configuration information
        this.inputPath = context.getConfiguration().get("inputPath");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.get() == 0) {
            // this is the first line of the file
            // Which files are we processing?
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            context.getCounter("FILE", filename).increment(1);
        }

	List<String> listOfStopWords = new ArrayList();
	listOfStopWords = Files.readAllLines(Paths.get("/home/hdshared/all_assignments-master/assignment4/stopwords.txt"));

        // 1. Eliminate non-alphabet characters
        String[] allStrings = value.toString().split("\\d+");

        for(String s : allStrings) {

	    // 1. Make all words lower case
	    s = s.toLowerCase();
	    // 1. Ignore stop words
	    String[] allWords = s.toLowerCase().split(" ");
 
    	    StringBuilder builder = new StringBuilder();
    	    for(String word : allWords) {
            	if(!listOfStopWords.contains(word)) {
            	    builder.append(word);
              	    builder.append(' ');
            	}
    	    }
     
    	    String result = builder.toString().trim();

	    String [] results = result.split("\\s+");	
	    for (String r : results) {		
		context.write(new Text(r), new Text(Integer.toString(i)));	
		i++;	
	    }  
	    
	}
    }
}
