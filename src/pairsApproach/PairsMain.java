package pairsApproach;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PairsMain extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int runJob = ToolRunner.run(new PairsMain(), args);
		System.exit(runJob);
	}
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"paircount");
		job.setJarByClass(this.getClass());
		
		job.setOutputKeyClass(StringPair.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setMapOutputKeyClass(StringPair.class);
	        
	    job.setMapperClass(PairsMap.class);
	    job.setReducerClass(PairsReduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    return job.waitForCompletion(true)?1:0;
		
		
	}

}
