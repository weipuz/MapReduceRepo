package org.CMPT732A1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	
	
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    	private int tmax_tmp;
    	
    	private Text word = new Text();
    	private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context)
           throws IOException, InterruptedException {
            String in = value.toString();
            String newin = in.replaceAll("[^a-zA-Z ]", "");
            StringTokenizer tokenizer = new StringTokenizer(newin);
            while(tokenizer.hasMoreTokens())
            {
            	word.set(tokenizer.nextToken());
    			context.write(word, one);
            	
            	
            }
            //context.write(new Text(in), one);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {
            // Write me
        	
        	int sum = 0;
        	while (values.iterator().hasNext())
        	{
        		sum += values.iterator().next().get(); 	
        	
        	}
        	//if (sum > 10000){
        	context.write(key, new IntWritable(sum));
        	//}
        }
    }

    public static void main(String[] args) throws Exception {
    	long starttime = System.currentTimeMillis();
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        long size = 1024*1024*20;
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", size);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        long endtime = System.currentTimeMillis();
        long time = endtime - starttime;
        System.out.println(time);
    }

}

