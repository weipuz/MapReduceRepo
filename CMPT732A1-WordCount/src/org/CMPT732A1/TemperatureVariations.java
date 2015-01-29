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

	private static String[] Africa = {"AG", "AO", "BC", "BN", "BY", "CD", "CN", "CT", "CV", "DJ", "EG", "EK", "ER", "ET", "GA", "GB", "GH", "GV", "KE", "LI", "LT", "LY", "MA", "MI", "ML", "MO", "MP", "MR", "MZ", "NG", "NI", "PP", "PU", "RE", "RW", "SE", "SF", "SG", "SL", "SO", "SU", "TO", "TP", "TS", "TZ", "UG", "UV", "WA", "WZ", "ZA", "ZI"};
    //1
    private static String[] Asia = {"AF", "BA", "BG", "BT", "BX", "CB", "CE", "CH", "CK", "HK", "ID", "IN", "IO", "IR", "IS", "IZ", "JA", "JO", "KG", "KT", "KU", "KZ", "LA", "LE", "MC", "MG", "MU", "MV", "MY", "NP", "PK", "PS", "QA", "RP", "SA", "SN", "SY", "TC", "TH", "TI", "TW", "TX", "UZ", "VM", "YE"};
    //2
    private static String[] Europe = {"AL", "AN", "AU", "BE", "BO", "BU", "CZ", "DA", "EI", "EN", "ES", "FG", "FI", "FO", "FP", "FR", "FS", "FY", "GC", "GE", "GI", "GP", "GR", "HR", "HU", "IC", "IT", "LG", "LH", "LO", "LS", "LU", "MB", "MD", "ME", "MN", "MT", "NA", "NC", "NL", "NO", "PL", "PO", "RE", "RO", "RS", "RU", "SB", "SI", "SJ", "SM", "SW", "SZ", "UK", "UP", "WF", "YU"};
    //3
    private static String[] NorthAmerica = {"AA", "AC", "AV", "BB", "BD", "BF", "BH", "CA", "CJ", "CS", "CU", "DO", "DR", "GJ", "GL", "GP", "GT", "HA", "HO", "JM", "MB", "MH", "MX", "NU", "PM", "RQ", "SB", "SC", "ST", "SV", "TD", "TK", "US", "VC", "VS"};
    //4
    private static String[] Oceania = {"AS", "AT", "BP", "CQ", "CR", "CW", "FJ", "FM", "FP", "GQ", "HQ", "IQ", "JQ", "KR", "NC", "NE", "NF", "NH", "NR", "NZ", "PN", "PP", "PS", "RM", "SS", "TK", "TN", "TV", "WF"};
    //5
    private static String[] SouthAmerica = {"AR", "BL", "BR", "CI", "CO", "EC", "FG", "FK", "GY", "NS", "PA", "PE", "UY", "VE"};

    public static class Map extends Mapper<LongWritable, Text, NCDCKey, FloatWritable> {
    	private int tmax_tmp;
    	private long date_tmp;
    	private String temp_station;
    	public static final Log LOG = LogFactory.getLog(Map.class);
        public void map(LongWritable key, Text value, Context context)
           throws IOException, InterruptedException {
            String in = value.toString();
            String[] inArrays = in.split("\n");
            //LOG.info("Map key "+ in);
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
	            		String continent = checkContinent(station);
	            		if(continent!=null){
	            		context.write(new NCDCKey(continent,inArray[1]) , new FloatWritable(diff));
	            		}
            		}
            	}
            }
        }
           // }
        private String checkContinent(String stat) {
			// TODO Auto-generated method stub
			for(int i=0; i<Africa.length; i++){
				if(stat.subSequence(0, 2).equals(Africa[i])){
					return "Africa";
				}
			}
			for(int i=0; i<Asia.length; i++){
				if(stat.subSequence(0, 2).equals(Asia[i])){
					return "Asia";
				}
			}
			for(int i=0; i<Europe.length; i++){
				if(stat.subSequence(0, 2).equals(Europe[i])){
					return "Europe";
				}
			}
			for(int i=0; i<Oceania.length; i++){
				if(stat.subSequence(0, 2).equals(Oceania[i])){
					return "Oceania";
				}
			}
			for(int i=0; i<NorthAmerica.length; i++){
				if(stat.subSequence(0, 2).equals(NorthAmerica[i])){
					return "NorthAmerica";
				}
			}
			for(int i=0; i<SouthAmerica.length; i++){
				if(stat.subSequence(0, 2).equals(SouthAmerica[i])){
					return "SouthAmerica";
				}
			}
			return null;
		}
		
	}
            
        
      



    public static class Reduce extends Reducer<NCDCKey, FloatWritable, NCDCKey, FloatWritable> {
        public void reduce(NCDCKey key, Iterable<FloatWritable> values, Context context)
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
        job.setMapOutputKeyClass(NCDCKey.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(NCDCKey.class);
        job.setOutputValueClass(FloatWritable.class);
        long size = 6710886;
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", size);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(NCDCPartitioner.class);
        job.setNumReduceTasks(6);

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
        NCDCKey date = new NCDCKey();
        FloatWritable diff = new FloatWritable();
        
    	try {
            BufferedWriter out = new BufferedWriter(new FileWriter("output_ave_new.csv"));
	            while(reader.next(date, diff)){ 
	                    out.write(diff.get() + ",\n");
	                    System.out.println(date.getFirst().toString()+" "+ diff.get());
	                }
	                out.close();
            } catch (IOException e) {}
        	
        	
       
    }



}
