import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

  
public class topN { 
  
    public static class Top_N_Mapper extends Mapper<Object, Text, Text, LongWritable> { 
  
    	private TreeMap<Long, String> map; 
    	  
        @Override
        public void setup(Context context) throws IOException, InterruptedException { 
            map = new TreeMap<Long, String>(); 
        } 
      
        @Override
        public void map(Object key, Text value, Context context) throws IOException,  InterruptedException { 

            String[] tokens = value.toString().split("\t|:|\\s"); 
            System.out.println(value.toString());
      
            String word = tokens[0]; 
            long num = Long.parseLong(tokens[2]); 
            
            if (tokens.length > 3) {
				for (int k = 3; k < tokens.length - 1; k += 2) {
					num += Long.parseLong(tokens[k+1]);
				}
            }
      
            map.put(num, word); 
      
            // we remove the first key-value 
            // if it's size increases 10 
            if (map.size() > 10) 
            { 
                map.remove(map.firstKey()); 
            } 
        } 
      
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException { 
            for (Map.Entry<Long, String> entry : map.entrySet()) { 
      
                String word = entry.getValue(); 
                long num = entry.getKey(); 
      
                context.write(new Text(word), new LongWritable(num)); 
            } 
        } 
    }
    
    public static class Top_N_Reducer extends Reducer<Text, LongWritable, LongWritable, Text> { 
  
    	private TreeMap<Long, String> result; 
    	  
        @Override
        public void setup(Context context) throws IOException, InterruptedException { 
            result = new TreeMap<Long, String>(); 
        } 
      
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException { 
            String name = key.toString(); 
            long count = 0; 
      
            for (LongWritable val : values) 
            { 
                count = val.get(); 
            } 
   
            result.put(count, name); 
            
            if (result.size() > 10) 
            { 
                result.remove(result.firstKey()); 
            } 
        } 
      
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException { 
            for (Map.Entry<Long, String> entry : result.entrySet())  { 
      
                long count = entry.getKey(); 
                String name = entry.getValue(); 
                context.write(new LongWritable(count), new Text(name)); 
            } 
        } 
     } 
    

    public static void main(String[] args) throws Exception 
    { 
    	Configuration conf = new Configuration(); 
    	conf.set("myValue", "10"); 
    	  
        String[] otherArgs = new GenericOptionsParser(conf, 
                                  args).getRemainingArgs(); 
  
        // if less than two paths provided will show error 
        if (otherArgs.length < 2)  
        { 
            System.err.println("Error: please provide two paths"); 
            System.exit(2); 
        } 
  
        Job job = Job.getInstance(conf, "TopN"); 
        job.setJarByClass(topN.class); 
  
        job.setMapperClass(Top_N_Mapper.class); 
        job.setReducerClass(Top_N_Reducer.class); 
  
        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(LongWritable.class); 
  
        job.setOutputKeyClass(LongWritable.class); 
        job.setOutputValueClass(Text.class); 
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } 
} 