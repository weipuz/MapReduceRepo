package org.CMPT732A1;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EEstimator extends Configured implements Tool {

    static private final Path TMP_DIR = new Path(
            EEstimator.class.getSimpleName() + "_TMP_EESTIMATOR");

    public static class EMapper
        extends Mapper<LongWritable, LongWritable, IntWritable, IntWritable> {
    	private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable seed, LongWritable size, Context context)
                throws IOException, InterruptedException {
            // TODO: write me
        	
        	//Random rd = new Random(seed.get()); 
        	MersenneTwister rd = new MersenneTwister(seed.get());
        	for(int i=0;i<size.get();i++){
        	    double sum = 0;
        	    int count = 0;
        		while(sum <= 1){
        			count ++;
        			sum += rd.nextDouble();
        		}
        		context.write(new IntWritable(count), one);
        		
        		
        	}
        }
    }

    public static class EReducer
        extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        public void reduce(IntWritable n, Iterable<IntWritable> ts, Context context)
                throws IOException, InterruptedException {
            // TODO: write me
        	
        	int sum = 0;
        	while (ts.iterator().hasNext())
        	{
        		sum += ts.iterator().next().get(); 	
        	
        	}
        	//if (sum > 10000){
        	context.write(n, new IntWritable(sum));
        	//}
        }
    }

    public static BigDecimal estimate(int numMaps, long numPoints, Job job)
            throws IOException, ClassNotFoundException, InterruptedException {

        // Job parameters
        job.setJarByClass(EEstimator.class);

        job.setMapperClass(EMapper.class);
        job.setCombinerClass(EReducer.class);
        job.setReducerClass(EReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setNumReduceTasks(1);

        // Input and output paths
        final Path inDir = new Path(TMP_DIR, "in");
        final Path outDir = new Path(TMP_DIR, "out");
        FileInputFormat.setInputPaths(job, inDir);
        FileOutputFormat.setOutputPath(job, outDir);

        // Create the temporary directory in which the input and output files are stored
        final FileSystem fs = FileSystem.get(job.getConfiguration());
        //FileContext fc = FileContext.getFileContext();
        if (fs.exists(TMP_DIR)) {
            throw new IOException("Temporary directory " + fs.makeQualified(TMP_DIR)
                    + " already exists.  Please remove it first.");
        }
        if (!fs.mkdirs(inDir)) {
            throw new IOException("Cannot create input directory " + inDir);
        }
        //Random rd = new Random(System.currentTimeMillis());
        
        MersenneTwister rd = new MersenneTwister(System.currentTimeMillis());
        // TODO: Generate one file for each map
        for (int i=0; i<numMaps; i++){
        	//try{
        	String filename = Integer.toString(i);
            Path Mapfile = new Path(inDir, filename);
        	SequenceFile.Writer sqwr = SequenceFile.createWriter(job.getConfiguration(), SequenceFile.Writer.file(Mapfile), SequenceFile.Writer.keyClass(LongWritable.class), SequenceFile.Writer.valueClass(LongWritable.class));
        	//String str = Long.toString(rd.nextLong()) + " " + Long.toString(numPoints) ;
        	sqwr.append(new LongWritable(rd.nextLong()), new LongWritable(numPoints));
        	sqwr.close();/*
        	}catch(Exception e){
        		System.out.println("File cant creat");	
        	}*/
        } 

        // Start the job
        System.out.println("Starting Job");
        final long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        final double duration = (System.currentTimeMillis() - startTime) / 1000.0;
        System.out.println("Job Finished in " + duration + " seconds");

        // Read the results and compute the value of Euler's constant
        SequenceFile.Reader reader =
            new SequenceFile.Reader(job.getConfiguration(), SequenceFile.Reader.file(new Path(TMP_DIR, "out/part-r-00000")));
        // TODO: Compute and return the result
        IntWritable n = new IntWritable();
        IntWritable feq = new IntWritable();
        long den = 0;
        long num = 0;
        while(reader.next(n, feq)){
        	den += feq.get();
        	System.out.println(n +" " + feq);
        	num += n.get() * feq.get();
        }
        
        return BigDecimal.valueOf( ((double)num / den));
    }

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: " + getClass().getName() + " <nMaps> <nSamples>");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        int nMaps = Integer.parseInt(args[0]);
        long nSamples = Long.parseLong(args[1]);

        System.out.println("Number of Maps  = " + nMaps);
        System.out.println("Samples per Map = " + nSamples);

        Job job = Job.getInstance(new Configuration());
        System.out.println("Estimated value of Euler's Constant = "
                           + estimate(nMaps, nSamples, job));
        return 0;
    }

    public static void main(String[] argv) throws Exception {
        System.exit(ToolRunner.run(null, new EEstimator(), argv));
    }
}

