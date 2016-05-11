package cscie63.assignment3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Problem3 {
    public static class MapClass extends 
		Mapper<Object, Text, IntWritable, Text> {
	    
	    public void map(Object key, Text value, Context context
	            ) throws IOException, InterruptedException {
//	        System.out.println(value);    
	        context.write(new IntWritable(Integer.parseInt(value.toString())), (Text)key);
	    }
	}	
	public static class ReduceClass 
		extends Reducer<IntWritable, Text, IntWritable, Text> {
	    
	    public void reduce(IntWritable key, Iterable<Text> values, 
	            Context context
	            ) throws IOException, InterruptedException {
	        int count = 0;   
	        for (Text val : values) {
	        	++ count;
	        }
	        context.write(key, new Text("" + count));
	    }
	}
	
	public static void main(String[] args) throws Exception { 
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: wordcount <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    Job job = Job.getInstance(conf, "word sequence");
	    job.setJarByClass(Problem3.class);
	    job.setMapperClass(MapClass.class);
	    job.setReducerClass(ReduceClass.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);	  
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
//        job.setMapOutputKeyClass(IntWritable.class);
//        job.setMapOutputValueClass(Text.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    for (int i = 0; i < otherArgs.length - 1; ++i) {
	      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	    }
	    FileOutputFormat.setOutputPath(job,
	      new Path(otherArgs[otherArgs.length - 1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
}
