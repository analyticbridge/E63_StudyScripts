package cscie63.assignment3;

import java.io.IOException;
//import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
// All below import statements are for old API
//import org.apache.hadoop.mapred.FileInputFormat;
//import org.apache.hadoop.mapred.FileOutputFormat;
//import org.apache.hadoop.mapred.JobClient;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.KeyValueTextInputFormat;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reducer;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapred.TextOutputFormat;
// These are for new API
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;

public class Problem5/*  extends Configured implements Tool*/  {
    public static class MapClass extends Mapper<Text, Text, Text, Text> {
    
	    public void map(Text key, Text value,
	                    Context context) throws IOException, InterruptedException {
	                    
	        context.write(value, key);
	    }
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
	    private Text text = new Text();
	    public void reduce(Text key, Iterable<Text> values,
	                       Context context) throws IOException, InterruptedException {
	        StringBuilder sb = new StringBuilder();
	        for (Text temp: values) {
	        	if (sb.length() > 0) sb.append(",");
	        	sb.append(temp.toString());
	        }
	        text.set(sb.toString());
	        sb = null;
	        context.write(key, text);
	    }
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("key.value.separator.in.input.line", ",");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
          System.err.println("Usage: Problem5 <in> [<in>...] <out>");
          System.exit(2);
        }
		
//	    Configuration conf = getConf();
	    // Old API
//    	JobConf job = new JobConf(conf, Inverter.class);
	    // new API
	    Job job = new Job(conf, "new inverter");	
	    job.setJarByClass(Problem5.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
          }
        FileOutputFormat.setOutputPath(job,
            new Path(otherArgs[otherArgs.length - 1]));
	    
	    job.setJobName("Inverter new API");
	    job.setMapperClass(MapClass.class);
	    job.setReducerClass(Reduce.class);
	    // Old API
//        job.setInputFormat(KeyValueTextInputFormat.class);
//        job.setOutputFormat(TextOutputFormat.class);	    
	    // New API
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    // Old API
//	    job.set("key.value.separator.in.input.line", ",");    
//	    JobClient.runJob(job);
	    // New API
	    job.waitForCompletion(true);	    
	}
	
/*	public static void main(String[] args) throws Exception { 
	    int res = ToolRunner.run(new Configuration(), new Problem5(), args);
	    
	    System.exit(res);
	}*/

}
