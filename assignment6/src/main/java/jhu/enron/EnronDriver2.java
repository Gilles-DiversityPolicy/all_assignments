package jhu.enron;

import jhu.avro.EmailSimple;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

;


/**
 * Created by wilsopw1 on 2/25/17.
 */
public class EnronDriver2 extends Configured implements Tool {

    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();
        String inputPath = conf.get("inputPath");
        String outputPath = conf.get("outputPath");
		
        Job job = Job.getInstance(conf, "Enron Processor 2");
        job.setJarByClass(getClass());

        System.out.printf("Enron Driver 2: %s %s\n", inputPath, outputPath);
		
		job.setMapperClass(EnronStatsMapper2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		job.setOutputValueClass(NullWritable.class);        
		job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setInputFormatClass(AvroKeyInputFormat.class);

		String commandType = conf.get("type");
	
		EnronStatsMapper2.type = commandType;
			
		if (commandType.equals("time")) {
			String start_date = conf.get("-start");
			String end_date = conf.get("-end");
			if (start_date != null)	EnronStatsMapper2.startDate = start_date;
			if (end_date != null) EnronStatsMapper2.endDate = end_date;
		}
		else if (commandType.equals("address")) {
			String from_address = conf.get("-from");
			String to_address = conf.get("-to");
			String cc_address = conf.get("-cc");
			String and = conf.get("-and");
			
			EnronStatsMapper2.fromAddressInQuery = from_address;
			EnronStatsMapper2.toAddressInQuery = to_address;
			EnronStatsMapper2.ccAddressInQuery = cc_address;
			EnronStatsMapper2.andFlag = (and != null);
		
		}
		else if (commandType.equals("subject")) {
			String inputRegex = conf.get("pattern");
			EnronStatsMapper2.regexForSubject = inputRegex;
		}
		else if (commandType.equals("body")) {
			String inputRegex = conf.get("pattern");
			EnronStatsMapper2.regexForBody = inputRegex;
		}
		else {
			return -1;
		}

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
