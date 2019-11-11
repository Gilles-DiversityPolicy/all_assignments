package jhu.enron;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.lang.*;
import java.time.LocalDate;
import java.time.ZonedDateTime;

/**
 * Created by wilsopw1 on 2/25/17.
 */
public class HistogramReducer extends Reducer<Text, Text, Text, IntWritable> {

	DateTimeFormatter emailFormatter = DateTimeFormatter.ofPattern("EEE, d MMM yyyy HH:mm:ss Z (z)");
	
	String commandDateFormat = "yyyy-mm-dd'T'HH";
	DateTimeFormatter commandFormatter = DateTimeFormatter.ofPattern(commandDateFormat);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
		
		String binType = key.toString();
		String inputDate = "";
		String timeQuant = null;
		if (binType.contains("hours")) timeQuant = "hours";
		else if (binType.contains("hour")) timeQuant = "hour";
		else if (binType.contains("days")) timeQuant = "days";		 
		else if (binType.contains("day")) timeQuant = "day";
		else if (binType.contains("months")) timeQuant = "months";		
		else if (binType.contains("month")) timeQuant = "month";
		else if (binType.contains("years")) timeQuant = "years";	
		else if (binType.contains("year")) timeQuant = "year";

		String[] numbers = binType.split("\\D++");
		int timeAmount = Integer.parseInt(numbers[0]);
		int year = Integer.parseInt(numbers[1]);
		int month = Integer.parseInt(numbers[2]);
		int day = Integer.parseInt(numbers[3]);
		int hours = Integer.parseInt(numbers[4]);
		
		inputDate += Integer.toString(year) + "-";
		inputDate += Integer.toString(month) + "-";
		inputDate += Integer.toString(day);
		
		boolean validISODate = false;
		boolean validISODateTime = false;
			
		for(TextWritable i : emailDates) {
			validISODate = false;
			validISODateTime = false;
			
			//////////////////////////////////////////////////////////////////////////////////////////
			//Check inputCommandDate and inputCommandDateTime
			LocalDate localDate = LocalDate.parse(inputDate);
			if (localDate != null) validISODate = true;				
			try {
				ZonedDateTime commandZonedDateTime = ZonedDateTime.parse(inputDate, commandFormatter);
				if (zonedDateTime != null) validISODateTime = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
			///////////////////////////////////////////////////////////////////////////////////////////
			
			//Next compare them within range
			if (validISODate || validISODateTime) {
				
				//Convert emailDate to zonedDateTime
				try {
					ZonedDateTime zonedDateTime = ZonedDateTime.parse(i.toString(), emailFormatter);
					if (zonedDateTime != null) validISODateTime = true;
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				//Compare inputCommandDate to 
				if (validISODate)
				{
					LocalDate emailDate = zonedDateTime.toLocalDate();
					
					if (localDate.equals(emailDate)) sum++;
				}
				if (validISODateTime)
				{
					
					if (commandZonedDateTime.equals(zonedDateTime)) sum++;
				}		
			}
			context.write(new Text(binType), new IntWritable(sum));
		}
    }
}
