// Greg Misicko
// Feb 19 2016
// e63 Big Data Analytics HW03 Problem 5
// This program is just an upgrade of Inverter. It removes all
// deprecated code. 

import java.io.IOException;
//import java.util.Iterator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

//import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapred.JobClient;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapred.KeyValueTextInputFormat;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Inverter2 extends Configured implements Tool {
	
	//   public static class MapClass extends MapReduceBase
    //   implements Mapper<Text, Text, Text, Text> {
       
    //   public void map(Text key, Text value,
    //                   OutputCollector<Text, Text> output,
    //                   Reporter reporter) throws IOException {
    
    public static class MapClass extends Mapper<Object, Text, Text, Text> {
        
        public void map(Object key, Text value,
                        Context context) throws IOException, InterruptedException {
                        
            //output.collect(value, key);
        	// input file is two numbers per line delimited by comma
        	String[] line = value.toString().split(",");
        	// swap the second number with the first
        	context.write(new Text(line[1]), new Text(line[0]));
        }
    }
    
 //   public static class Reduce extends MapReduceBase
 //       implements Reducer<Text, Text, Text, Text> {
      public static class Reduce extends Reducer<Text, Text, Text, Text> {
        
        //public void reduce(Text key, Iterator<Text> values,
        //                   OutputCollector<Text, Text> output,
        //                   Reporter reporter) throws IOException {
    	public void reduce(Text key, Iterable<Text> values, 
                Context context) throws IOException, InterruptedException {
                           
            String csv = "";
            //while (values.hasNext()) {
            for (Text val : values) {
                if (csv.length() > 0) csv += ",";
                //csv += values.next().toString();
                csv += val.toString();
            }
            //output.collect(key, new Text(csv));
            context.write(key, new Text(csv));
        }
    }
    
    public int run(String[] args) throws Exception {
        //Configuration conf = getConf();
    	Configuration conf = new Configuration();
        
        //JobConf job = new JobConf(conf, Inverter2.class);
        Job job = Job.getInstance(conf, "inverter");
        
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setJarByClass(Inverter2.class);
        
        //job.setJobName("Inverter");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        
        //job.setInputFormat(KeyValueTextInputFormat.class);
        //job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.set("key.value.separator.in.input.line", ",");
        
        //JobClient.runJob(job);
        System.exit(job.waitForCompletion(true)?0:1);
        
        return 0;
    }
    
    public static void main(String[] args) throws Exception { 
        int res = ToolRunner.run(new Configuration(), new Inverter2(), args);
        
        System.exit(res);
    }
}