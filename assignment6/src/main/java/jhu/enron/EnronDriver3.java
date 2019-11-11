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
public class EnronDriver3 extends Configured implements Tool {

    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();
        String inputPath = conf.get("inputPath");
        String outputPath = conf.get("outputPath");
		
        Job job = Job.getInstance(conf, "Enron Processor 3");
        job.setJarByClass(getClass());

        System.out.printf("Enron Driver 3: %s %s\n", inputPath, outputPath);
		
		job.setMapperClass(EnronStatsMapper3.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(HistogramReducer.class);
		job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setInputFormatClass(AvroKeyInputFormat.class);

		String inputBinType = conf.get("type");
		EnronStatsMapper3.binType = inputBinType;

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
