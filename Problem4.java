package cscie63.assignment3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import cscie63.assignment3.Problem1.IntSumReducer;
import cscie63.assignment3.Problem1.TokenizerMapper;
import cscie63.assignment3.Problem3.MapClass;
import cscie63.assignment3.Problem3.ReduceClass;

public class Problem4 {
	public static void main(String[] args) throws IOException, 
			ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: Problem4 <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    Job job = Job.getInstance(conf, "wordcount again");
	    job.setJarByClass(Problem1.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    for (int i = 0; i < otherArgs.length - 1; ++i) {
	      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	    }
	    FileOutputFormat.setOutputPath(job, new Path("temp"));
	    
	    job.waitForCompletion(true);
	    
	    job = Job.getInstance(conf, "sort descending again");
	    job.setJarByClass(Problem3.class);
	    job.setMapperClass(MapClass.class);
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    job.setReducerClass(ReduceClass.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path("temp"));
	    FileOutputFormat.setOutputPath(job,
	      new Path(otherArgs[otherArgs.length - 1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    
	}
}
