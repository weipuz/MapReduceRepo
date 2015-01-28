package org.CMPT732A1;

import java.io.*;
import java.util.*;
import java.io.BufferedReader;

import org.CMPT732A1.TemperatureVariations.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.util.BufferedReader;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class NCDCRecordInputFormat extends TextInputFormat {
    public static final Log LOG = LogFactory.getLog(Map.class);

    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new NCDCRecordReader();
    }
    
    public class NCDCRecordReader extends RecordReader<LongWritable, Text> {
        //private BufferedReader in;
        private LineReader in ; 
        private long start, end;
        private long pos=0;
        //private long date_tmp;
       // private String station_tmp;
        private int maxLineLength;
        private LongWritable currentKey = new LongWritable();
        private Text currentValue = new Text();
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
           //initialized run once per split
            String line;
           Configuration job = context.getConfiguration();
           this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);

           // Open the file.
           FileSplit fileSplit = (FileSplit)split;
           Path file = fileSplit.getPath();
           FileSystem fs = file.getFileSystem(job);
           FSDataInputStream is = fs.open(file);
           //in = new BufferedReader(new InputStreamReader(is));
           // Find the beginning and the end of the split.
           start = fileSplit.getStart();
           end = start + fileSplit.getLength();
          // date_tmp = 0;
           //station_tmp = null;
           //System.out.println(Long.toString(start) +" end: "+ Long.toString(end));
           //LOG.info(Long.toString(start) +" end: "+ Long.toString(end));
           // TODO: write the rest of the function. It will initialize needed
           // variables, move to the right position in the file, and start
           // reading if needed.
           boolean skipFirstRecord = false;
           
           //check if last split has already read some part of this file.
           if (start != 0){
        	   skipFirstRecord = true;
               --start;
               is.seek(start);
           }
           in = new LineReader(is,job);
           if(skipFirstRecord){
        	   
        	   start += in.readLine(new Text());
        	 
               
           }
           this.pos = start;
}

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
                   // TODO: read the next key/value, set the key and value variables
                   // to the right values, and return true if there are more key and
                   // to read. Otherwise, return false.
            long date_tmp = 0;
            String station_tmp = new String();
            station_tmp = null;
             if (currentKey == null) {
                 currentKey = new LongWritable();
                }
             currentKey.set(pos);
             if (currentValue == null) {
                 currentValue = new Text();
                }
             currentValue.clear();
             final Text endline = new Text("\n");
             int newSize = 0;
             int newSize2 = 0;
             boolean nonewrecord = true;
             
             if(pos > end){
            	 currentKey = null;
                 currentValue = null;
                 return false;
            	 
             }
             
             Text v = new Text();
             Text v2 = new Text();
             
             
             while(nonewrecord && (pos < end)){
                    newSize = in.readLine(v, maxLineLength,maxLineLength);        
                       // LOG.info(v.toString() +" first read "+ Integer.toString(newSize));
                    if(newSize == 0){
                    	
                    	break;
                    }
                    
                    String[] vArray = v.toString().split(",");
                    
                    if(vArray.length < 3){
                    	nonewrecord = true;
                    	pos += newSize;
                    	continue;
                    }
                    else if(vArray[2].equals("TMAX")){
                        	Text buffer = new Text();
                    		date_tmp = Long.parseLong(vArray[1]);
	                        station_tmp = vArray[0];
	                        
	                        pos += newSize;
	                        
	                        newSize2 = in.readLine(v2, maxLineLength,maxLineLength);
	                       // LOG.info(v2.toString() +" second read "+ Integer.toString(newSize2));
	                        String[] vArray2 = v2.toString().split(",");
	                        if(vArray2.length < 3){
	                        	nonewrecord = true;
	                        	pos += newSize2;
	                        	continue;
	                        }
	                        else if(vArray2[2].equals("TMIN") && date_tmp==Long.parseLong(vArray2[1]) && station_tmp.equals(vArray2[0])){
	                        	nonewrecord = false;
	                        	currentValue.append(v.getBytes(),0, v.getLength());
	                        	currentValue.append(endline.getBytes(),0, endline.getLength());
	                        	currentValue.append(v2.getBytes(),0, v2.getLength());
	                        	currentValue.append(endline.getBytes(),0, endline.getLength());
	                        	pos += newSize2;
	                        	//LOG.info(v2.toString() +" second read "+ Integer.toString(newSize2));
	                            continue;
	                        }
	                        else{
	                        	pos += newSize2;
	                        	continue;
	                        }
                    
                    }
                    else{
                    	pos += newSize;
                    	continue;
                    }
               }
             
             
             
            if (nonewrecord) {
                currentKey = null;
                currentValue = null;
                return false;
            } else {
                return true;
            }
         
             
             
        }
       @Override
       public void close() throws IOException {
           if (in != null) {
                in.close();
            }
       }
       @Override
       public LongWritable getCurrentKey() throws IOException, InterruptedException {
           return currentKey;
       }
       @Override
       public Text getCurrentValue() throws IOException, InterruptedException {
           return currentValue;
       }
       @Override
       public float getProgress() throws IOException, InterruptedException {
           // TODO: calculate a value between 0 and 1 that will represent the
           // fraction of the file that has been processed so far.
           if (start == end) {
               return 0.0f;
           }
           else {
               return Math.min(1.0f, (pos - start) / (float)(end - start));
           }
       }
} 
}