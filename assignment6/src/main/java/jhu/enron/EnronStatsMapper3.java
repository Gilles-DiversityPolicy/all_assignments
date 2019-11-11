package jhu.enron;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import jhu.avro.EmailSimple;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by wilsopw1 on 2/25/17.
 */
public class EnronStatsMapper3 extends Mapper<AvroKey<EmailSimple>, NullWritable,
        Text, IntWritable> {
    
	Gson gson = new GsonBuilder().create();
    String inputPath = null;
    IntWritable One = new IntWritable(1);
	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE, d MMM yyyy HH:mm:ss Z (z)");
	public static String binType = null;
	
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
		
    }

    @Override
    protected void map(AvroKey<EmailSimple> key, NullWritable value, Context context) throws IOException, InterruptedException {
				
        EmailSimple email = key.datum();		//Assign Email object
		CharSequence date = email.getDate(); 	//E-mail address in "Date" section
        if(date != null) {
            try {
				ZonedDateTime zonedDateTime = ZonedDateTime.parse(date, formatter);
				context.write(new Text(binType), new Text(date.toString()));
			} catch (Exception e) {
				e.printStackTrace();
			}	
		}	
        
    }
}