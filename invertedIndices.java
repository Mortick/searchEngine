import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class invertedIndices {
    public static class indexMapper extends Mapper<Object, Text, Text, Text> {
        private Text filename = new Text();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String file = ((FileSplit) context.getInputSplit()).getPath().toString();
            filename.set(file);
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f\",.:;?![]()'-"); 
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, filename);
            }
        }
    }

    public static class indexReducer extends Reducer<Text, Text, Text, Text> {
        HashMap<String, Integer> results;
        private Text text = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            results = new HashMap<String, Integer>();
            StringBuilder builder = new StringBuilder();

            for (Text val : values) {
                if (results.containsKey(val.toString())) {
                    results.put(val.toString(), results.get(val.toString()) + 1);
                } else {
                    results.put(val.toString(), 1);
                }
            }

            for(String filename : results.keySet()) {
                builder.append(filename + ":" + results.get(filename));
            }
            text.set(builder.toString());
            context.write(key, text);
        }
    }

    public static void main(String[] args) throws Exception 
    { 
        Configuration conf = new Configuration(); 
        Job job = Job.getInstance(conf, "inverted_indices"); 
        job.setJarByClass(invertedIndices.class); 
        job.setMapperClass(indexMapper.class); 
        job.setReducerClass(indexReducer.class); 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(Text.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
}