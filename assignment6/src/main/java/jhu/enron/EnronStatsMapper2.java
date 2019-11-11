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
public class EnronStatsMapper2 extends Mapper<AvroKey<EmailSimple>, NullWritable,
        Text, IntWritable> {
    
	Gson gson = new GsonBuilder().create();
    String inputPath = null;
    IntWritable One = new IntWritable(1);
	
	public static String type = null;
	public static String startDate = null;
	public static String endDate = null; 
	public static String fromAddressInQuery = null;
	public static String toAddressInQuery = null;
	public static String ccAddressInQuery = null;
	public static String regexForSubject = null;	
	public static String regexForBody = null;
	public static boolean andFlag = false;
	
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
		
    }

    @Override
    protected void map(AvroKey<EmailSimple> key, NullWritable value, Context context) throws IOException, InterruptedException {
		
		//Attempt 2: Parse configuration parameters here
		
        EmailSimple email = key.datum();		//Assign Email object
		CharSequence date = email.getDate(); 	//E-mail address in "Date" section
        if(date != null)
            context.write(new Text(date.toString()), One);
        CharSequence from = email.getFrom(); 	//E-mail address in "From" section
        if(from != null)
            context.write(new Text(from.toString()), One);
        List<CharSequence> to = email.getTo();	//E-mail address(es) in "To" section
        if(to != null)
            for(CharSequence e : to)
                context.write(new Text(e.toString()), One);
        List<CharSequence> cc = email.getCc();	//E-mail address(es) in "Cc" section
        if(cc != null)
            for(CharSequence e : cc)
                context.write(new Text(e.toString()), One);
        List<CharSequence> bcc = email.getBcc();//E-mail address(es) in "Bcc" section
        if(bcc != null)
            for (CharSequence e : bcc)
                context.write(new Text(e.toString()), One);
			
		//NEXT UP...
		
		//Text in in "Subject" section
		CharSequence subject = email.getSubject(); 	//E-mail address in "Date" section
        if(subject != null)
            context.write(new Text(subject.toString()), One);
		//Text in in "Body" section
    }
}