package stripeApproach;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;



public class StripeMain extends Configured implements Tool  {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf(), "StripeJob");
		job.setJarByClass(this.getClass());
	return 0;
	}

}
