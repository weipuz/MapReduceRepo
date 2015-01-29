package org.CMPT732A1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class NCDCPartitioner extends Partitioner<NCDCKey, FloatWritable> {

	@Override
	public int getPartition(NCDCKey key, FloatWritable tempDiffs, int numReduceTasks) {
		// TODO Auto-generated method stub
		//String [] contDate = key.getFirst().toString().split(",");
		String continent = key.getFirst().toString();
		
		if(numReduceTasks == 0){
			return 0;
		}
		
		if(continent.equals("Africa")){
			return 0;
		}else if (continent.equals("Asia")){
			return 1;
		}else if (continent.equals("Europe")){
			return 2;
		}else if (continent.equals("NorthAmerica")){
			return 3;
		}else if (continent.equals("Oceania")){
			return 4;
		}else {
			return 5;
		}
	}


}
