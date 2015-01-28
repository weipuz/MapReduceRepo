package org.CMPT732A1;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TemperatureVariations {

	

    public static class Map extends Mapper<LongWritable, Text, LongWritable, FloatWritable> {
    	private int tmax_tmp;
    	private long date_tmp;
    	private String temp_station;
    	public static final Log LOG = LogFactory.getLog(Map.class);
        public void map(LongWritable key, Text value, Context context)
           throws IOException, InterruptedException {
            String in = value.toString();
            String[] inArrays = in.split("\n");
            LOG.info("Map key "+ in);
            for(int i=0;i<inArrays.length; i++){
           // if (inArray[0].equals("USW00094728"))
           // {
            	String[] inArray = inArrays[i].split(",") ;
            	if(inArray[2].equals("TMAX"))
            	{
            		tmax_tmp = Integer.parseInt(inArray[3]);
            		date_tmp = Long.parseLong(inArray[1]);
            		temp_station = inArray[0];
            		
            	}
            	if(inArray[2].equals("TMIN"))
            	{
            		long date = Long.parseLong(inArray[1]);
            		String station = inArray[0];
            		if (date == date_tmp && station.equals(temp_station))
            		{
            			
	            		int tmin = Integer.parseInt(inArray[3]);
	            		float diff = (float)(tmax_tmp-tmin)/(float)10; 
	            		
	            		context.write(new LongWritable(date) , new FloatWritable(diff));
            		}
            	}
            }
        }
           // }
            
        
      }



    public static class Reduce extends Reducer<LongWritable, FloatWritable, LongWritable, FloatWritable> {
        public void reduce(LongWritable key, Iterable<FloatWritable> values, Context context)
          throws IOException, InterruptedException {
            // Write me
        	
        	double sum = 0;
        	int count = 0;
        	while (values.iterator().hasNext())
        	{
        		sum += values.iterator().next().get(); 	
        		count ++;
        	
        	}
        	
        	context.write(key, new FloatWritable((float)sum/count));
        	
        }
     }
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		
		long starttime = System.currentTimeMillis();
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TemperatureVariations.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        long size = 6710886;
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", size);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(NCDCRecordInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        long endtime = System.currentTimeMillis();
        long time = endtime - starttime;
        System.out.println(time);
        
        SequenceFile.Reader reader =
                new SequenceFile.Reader(job.getConfiguration(), SequenceFile.Reader.file(new Path(args[1] + "/part-r-00000")));
        LongWritable date = new LongWritable();
        FloatWritable diff = new FloatWritable();
        
    	try {
            BufferedWriter out = new BufferedWriter(new FileWriter("output_ave_new.csv"));
	            while(reader.next(date, diff)){ 
	                    out.write(diff.get() + ",\n");
	                    System.out.println(diff.get());
	                }
	                out.close();
            } catch (IOException e) {}
        	
        	
       
    }



}
