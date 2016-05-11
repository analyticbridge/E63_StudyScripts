package cscie63.assignment3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Problem2 {
    
    public static class MapClass extends 
    	Mapper<Object, Text, IntWritable, Text> {
        
        public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
//            System.out.println(value);    
            context.write(new IntWritable(Integer.parseInt(value.toString())), (Text)key);
        }
    }
    
    public static class ReduceClass 
    	extends Reducer<IntWritable,Text,IntWritable, Text> {
        
        public void reduce(IntWritable key, Iterable<Text> values, 
                Context context
                ) throws IOException, InterruptedException {
                           
            for (Text val : values) {
            	context.write(key, val);
            }
        }
    }
    
    public static class OrderKeyComparator extends WritableComparator {
        protected OrderKeyComparator() {
            super(IntWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
        	IntWritable key1 = (IntWritable) w1;
        	IntWritable key2 = (IntWritable) w2;          
            return -1 * key1.compareTo(key2);
        }
    }   
    
    public static void main(String[] args) throws Exception { 
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
          System.err.println("Usage: Problem2 <in> [<in>...] <out>");
          System.exit(2);
        }
        Job job = Job.getInstance(conf, "sort descending");
        job.setJarByClass(Problem2.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(OrderKeyComparator.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
          FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
          new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

