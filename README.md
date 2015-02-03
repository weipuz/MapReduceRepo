# MapReduceRepo
##MapReducePractice for CMPT732 Assignment 1  
Author: Weipu ZHAO
###1 Word Count, Your First Hadoop Programt
CMPT732A1-WordCount/src/org/CMPT732/WordCount.java

basic map reduce implementation

###2 MapReduce for Parallelizing Computations
CMPT732A1-WordCount/src/org/CMPT732/EEstimator.java

We will now estimate the value of Euler’s constant (e) using a Monte Carlo method. Let X1, X2, ..., Xn be an
infinite sequence of independent random variables drawn from the uniform distribution on [0;1]. Let V be the
least number n such that the sum of the first n samples exceeds 1:

V = minfn j X1+X2+:::+Xn > 1g

The expected value of V is e:

E(V) = e

Each Map task will generate random points using a uniform distribution on [0;1] in order to find a fixed
number of values of n. It will output the number of time each value of n has been produced. The Reduce task
will sum the results, and using them, the program will calculate the expected value of V and print the result.

To solve this, we create one sequence file as input of each map task. In that sequence file we specific a pair of number for map task. Every pair contains the seed for that map task, and the number of samples needed per map task. 

In order to make sure each map task using different seed, we use Random rd = new Random(System.currentTimeMillis()); to create random long integers for each map task as its seed. The input of map is LongWritable seed and LongWritable size. Seed is the seed of random function of that map task, size is the number of samples per map task.

To run the code:
<pre><code>hadoop jar EulersConstant.jar org.CMPT732A1.EulersConstant 10 100000
</code></pre>
The first parameter is the number of Mappers, and the second parameter is the number of values each Mapper
will produce for n.
###3 NCDC Weather Data
THis is about the process and anaylz of NCDC weather data. The NCDC produces CSV (Comma-Separated Values) files with worldwide weather data for
each year. Each line of one of these files contains:
- The weather station’s code.
- The date, in the ISO-8601 format.
- The type of value stored in that line. All values are integers. TMIN (resp. TMAX) stands for minimum
(resp. maximum) temperature. Temperatures are expressed in tenth of degrees Celsius. AWND stands for
average wind speed, and PRCP stands for precipitation (rainfall), etc. Several other types of records are
used (TOBS, SNOW, ...).
- The next field contains the corresponding value (temperature, wind speed, rainfall, etc.)
- All lines contain five more fields that we won’t use in this exercise.

We will work on the CSV file for 2013, which has been sorted by date first, station second, and value type
third, in order to ease its parsing.

>AE000041196,20130101,TMAX,250,,,S,

>AG000060390,20130101,PRCP,0,,,S,

>AG000060390,20130101,TMAX,171,,,S,

>AG000060590,20130101,PRCP,0,,,S,

>AG000060590,20130101,TMAX,170,,,S,
...

####1.Temperature Variations

CMPT732A1-WordCount/src/org/CMPT732/TemperatureVariations.java

Plot the difference between the maximum and the minimum temperature in Central Park for each day in 2013. There is a weather station in Central Park: its code is USW00094728.
>USW00094728,20130101,TMAX,44,,,X,2400
USW00094728,20130101,TMIN,-33,,,X,2400
USW00094728,20130102,TMAX,6,,,X,2400
USW00094728,20130102,TMIN,-56,,,X,2400
USW00094728,20130103,TMAX,0,,,X,2400
USW00094728,20130103,TMIN,-44,,,X,2400
...

In order to make the output in order, we take the date as key, the temperature difference as the value. In every map, it stores the TMAX for the same day and emit the difference while read the TMIN.  It is assumed that TMAX record always precedes the TMIN in NCDC’s data, and not split occors between a TMAX and a TMIN record.

The first few lines of output:
>- 7.7,
>- 11.6,
>- 4.4,
...


####2.Average Temperature Variations

Write a Reducer that can compute the average worldwide temperature variations. We modified the mapper and reducer from previous question, make mapper emit every difference which have both TMAX and TMIN record with same date as key, and sum and compute average during the reducer. If a station doesn’t provide both the minimum or maximum temperature for that day, it will be ignored.

The resule start with:
>- 9.857165, 
>- 9.882375, 
>- 10.542754...

####3.Custom RecordReader

The previous solution have a problem: when we change the split size, the records near the split boundary could be incomplete, in this situation we simply discarded it. Thus the result of different split size various.

To solve the issue, we’re going to write a custom InputFormat and a custom RecordReader. Idealy these classes will split the input files by record, instead of lines: each record will be a series of lines that contain all data from one station for a given day.

Regarding this question, we implemented two versions of recordreader.

1) For the simple implementation:<pre><code>CMPT732A1-WordCount/src/org/CMPT732/NCDCRecordInputFormat.java</code></pre> 
In the simple implementation, we read only the record contains TMAX and TMIN record. In another word, apart from compare station key and date key of each line, we just consider TMAX as beginning of each record and TMIN as end of each record. In this way, we discard all the records incomplete, and every read within every split ALWAYS start with TMAX and also end with TMIN. 

This take care of follow situation:
- Split between TMAX and TMIN: TMIN will read through internet by previous split. For the new split we will always start reading with TMAX so record overlap.
- Split between same record but TMAX and TMIN in the same split: For both situation, TMAX and TMIN will be read as one record within their split.
- Record only contain TMAX or TMIN: 
		
This didn’t take care:
- TMIN happened before TMAX with same station and date. 

The results of this implementation remain same while change split size. 


2) Beyond this implementation, we implemented a more complex one who can read all the lines of same station same date as one record. 
<pre><code>CMPT732A1-WordCount/src/org/CMPT732/NCDCRecordInputFormatNew.java</code></pre> 

The primary principles were to make sure:
- Every split read a complete record at the end of this split, i.e., continue read some contents of next split through internet until new record;
- Every split read start from a new record, which means, we read one line backward, even cross the split, and compare the line before with current line. Keep read until new record pops out.  

A very tricky thing here is we need to keep in mind the file read pointer, especially when we read backward, we need read character by character, everytime we move the position pointer one byte backward and reconstruct a new file reader using the moved file stream. We use seek() to move the file input stream. 

Also, if we already in the middle of one record, before we read next line from a reader, we need to mark its current position using reader.mark(). This is to make sure this reader can be reseted (move back the pointer and read this line next time as beginning of a new record) if the line read is a new record.

####4.Custom key class and custom Partitioner

You are now asked to plot temperature variations for each continent. The first two letters of
each weather station’s code indicates which country it’s based in. In the files provided with the assignment,
you will find the text file country_codes.txt which contains a list of country codes for each continent:

>Africa:AG,AO,BC,BN,BY,CD,CN,CT,CV,DJ,EG,EK,ER,ET,GA,GB,GH,GV,KE,LI,LT,LY,MA,MI,ML,MO,MP,MR,MZ,NG,NI,PP,PU,RE,RW,SE,SF,SG,SL,SO,SU,TO,TP,TS,TZ,UG,UV,WA,WZ,ZA,ZI

>Asia:AF,BA,BG,BT,BX,CB,CE,CH,CK,HK,ID,IN,IO,IR,IS,IZ,JA,JO,KG,KT,KU,KZ,LA,LE,MC,MG,MU,MV,MY,NP,PK,PS,QA,RP,SA,SN,SY,TC,TH,TI,TW,TX,UZ,VM,YE

>...

We will use this information to make one Reduce task calculate the averages for each continent. We creat two files:
<pre><code>CMPT732A1-WordCount/src/org/CMPT732/NCDCPartitioner.java
CMPT732A1-WordCount/src/org/CMPT732/NCDCKey.java</code></pre>

The key of mapper output should contain two informations: the continent and the date. The NCDCKey class is to implement the WritableComparable interface to make sure this key can convey two pair of text. After that, the NCDCPartitioner class can take the first element of key class and do partition based on that.
