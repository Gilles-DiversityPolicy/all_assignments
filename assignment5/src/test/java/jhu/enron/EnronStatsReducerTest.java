package jhu.enron;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by wilsopw1 on 2/26/17.
 */
public class EnronStatsReducerTest {
    ReduceDriver<Text, Text, Text, Text> reduceDriver;

    @Before
    public void setUp() throws Exception {
        EnronStatsReducer reducer = new EnronStatsReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void reduce() throws Exception {
	        
	/*List<Text> values = new ArrayList<Text>();
       
        values.add(new Text("To"));
        values.add(new Text("Cc"));
	values.add(new Text("Cc"));

        reduceDriver.withInput(new Text("Sender1" + "->" + "Recipient1"), values);
        reduceDriver.withOutput(new Text("Sender1" + "->" + "Recipient1"), new Text("<Sender1> <Recipient1> <1> <2>"));
        
	reduceDriver.withOutput(new Text("To"), new Text("Recipient2"));
        reduceDriver.withInput(new Text("From"), new Text("Sender2"));
        reduceDriver.withOutput(new Text("To"), new Text("Recipient3"));
        reduceDriver.withOutput(new Text("To"), new Text("Recipient4"));

        reduceDriver.runTest();*/
	
    }

}
