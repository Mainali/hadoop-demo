package pairsApproach;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class PairsMap extends Mapper<LongWritable, Text, StringPair, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private final StringPair pair = new StringPair();
	public void map(LongWritable key,Text val, Mapper<LongWritable, Text, StringPair, IntWritable>.Context context) throws IOException, InterruptedException{
		String line = val.toString();
		String[] items = line.split("\\s+");
		
		for(int i=0;i<items.length;i++){
		
			int j = i+1;
			while(!(items[j].equals(items[i])) && j<items.length){
				pair.setPairs(items[i], items[j]);
				context.write(pair, one);
				pair.setPairs(items[i], "*");
				context.write(pair,one);
			}
		}
	}

}
