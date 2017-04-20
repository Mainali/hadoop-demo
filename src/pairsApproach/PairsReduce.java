package pairsApproach;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PairsReduce extends Reducer<StringPair, IntWritable, StringPair, DoubleWritable> {
	public double total;
	
	
	public void setup(Reducer<StringPair,IntWritable,StringPair,DoubleWritable>.Context context) throws IOException, InterruptedException{
		super.setup(context);
		total = 0.0;
	}

	public void reduce(StringPair key,Iterable<IntWritable> vals,Reducer<StringPair,IntWritable,StringPair,DoubleWritable>.Context context) throws IOException, InterruptedException{
		int s=0;
		for(IntWritable v :vals){
			s +=v.get();
		}
		
		if(key.getNeighbour().toString().equals("*")){
			total = s;
		}else{
			double result = s/total;
			context.write(key, new DoubleWritable(Double.valueOf(result)));
		}
	}
}
