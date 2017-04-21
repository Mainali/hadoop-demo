package stripeApproach;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class StripeMain extends Configured implements Tool  {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new StripeMain(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf(), "StripeJob");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		job.setMapperClass(StripeMapper.class);
		job.setReducerClass(StripeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritableExtended.class);
		job.setMapOutputValueClass(MapWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
