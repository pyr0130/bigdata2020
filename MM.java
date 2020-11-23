import java.util.HashMap;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMultiply {
	
	
  public static class MatrixMap 
  		extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException {
    	
    	Configuration conf = context.getConfiguration();
    	
    	int l = Integer.parseInt(conf.get("l"));
    	int m = Integer.parseInt(conf.get("m"));
    	int n = Integer.parseInt(conf.get("n"));
    	
    	String line = value.toString();
        int endIndex = line.length()-1;
    	String sub_line = line.substring(1,endIndex);
    	String[] line_arr = sub_line.split(",");
	
	String mat_name = line_arr[0].substring(1,2); 
    	String row = line_arr[1].replaceAll(" ","");
    	String col = line_arr[2].replaceAll(" ","");
    	String val = line_arr[3].replaceAll(" ","");
	
    	Text outKey = new Text();
    	Text outValue = new Text();
    	
    	if (mat_name.equals("a")) {
    		for (int i=0;i<n;i++) {
    			outKey.set(row+","+i);
    			outValue.set("a,"+col+","+val);
    			context.write(outKey, outValue);
   		}
    	}else {
    		for (int i=0; i<l;i++) {
    			outKey.set(i + ","+ col);
    			outValue.set("b,"+row+","+val);
    			context.write(outKey, outValue);
    		}
    	}
    	
    }
  }
  public static class MultiplyReducer
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
      Configuration conf = context.getConfiguration();
      
      HashMap<Integer, Double> hash_a = new HashMap<Integer, Double>();
      HashMap<Integer, Double> hash_b = new HashMap<Integer, Double>();
      
      for (Text val : values) {
    	  String[] value = val.toString().split(",");
    	  if(value[0].equals("a")) {
    		  hash_a.put(Integer.parseInt(value[1]), Double.parseDouble(value[2]));
    	  }else {
    		  hash_b.put(Integer.parseInt(value[1]), Double.parseDouble(value[2]));
    	  }
      }
      
      int l = Integer.parseInt(conf.get("l"));
      int m = Integer.parseInt(conf.get("m"));
      int n = Integer.parseInt(conf.get("n"));
  	  
      double res = 0.0;
  	  
      for (int i=0;i<m;i++) {
  	  double a_ij = hash_a.containsKey(i) ? hash_a.get(i) : 0.0;
  	  double b_ij = hash_b.containsKey(i) ? hash_b.get(i) : 0.0;
  	  res += a_ij * b_ij;
      }
      if (res != 0.0f) {
    	  context.write(key, new Text("" + res));
      }
      
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    conf.set("l","5");
    conf.set("m","5");
    conf.set("n","5");
    
    Job job = Job.getInstance(conf, "matrix multiplication");
    job.setJarByClass(MatrixMultiply.class);
    job.setMapperClass(MatrixMap.class);
    // to reduce network bottleneck
    job.setReducerClass(MultiplyReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
