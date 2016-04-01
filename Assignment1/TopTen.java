import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class TopTen {

	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable>{
		
		public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		
			String[] mydata = value.toString().split("::");
			if (mydata.length > 23){
				if("review".compareTo(mydata[22])== 0){
						Float new_Variable=Float.parseFloat(mydata[20].toString());
						context.write(new Text(mydata[2]),new FloatWritable(new_Variable));
						
				}
			}
			
		}		
		
		@Override
		protected void setup(Context context)
			throws IOException, InterruptedException {
		}
	}
	
	public static class Reduce extends Reducer<Text,FloatWritable,Text,Text> {
		private  static HashMap<String,Float> ratingMap= new HashMap<String,Float>();
		
		public void reduce(Text key, Iterable<FloatWritable> values,Context context ) throws IOException, InterruptedException {
			int  count =0;
			Float sum = 0.0f;
			Float avg = 0.0f;
			for(FloatWritable iterator:values){
				sum = sum + iterator.get();
				count = count +1;
			}
			avg = sum /count;
			String reviewid = key.toString();
			ratingMap.put(reviewid, avg);
			//System.out.println("id"+reviewid+"key"+avg);
			//context.write(new Text(key),new Text(avg.toString()));
			
		}
		
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException {
			
			Integer topTenReview = 0;
			HashMap<String,Float> sortedMap = sortByComparator(ratingMap);
			for (Entry<String,Float> tempvalue : sortedMap.entrySet()){
				context.write(new Text (tempvalue.getKey()),new Text(tempvalue.getValue().toString()));
				topTenReview++;
				if(topTenReview>=10)
					break;
			}
			
		}
		
		
		 private static HashMap<String, Float> sortByComparator(HashMap<String, Float> unsortMap){
			 				 
			 	//System.out.println("unsorted value");		 	
		        LinkedList<Entry<String,Float>> list = new LinkedList<Entry<String, Float>>(unsortMap.entrySet());
		        // Sorting the list based on values
		        Collections.sort(list, new Comparator<Entry<String, Float>>()
		        {    
					@Override
					public int compare(Entry<String, Float> o1,
							Entry<String, Float> o2) {
						// TODO Auto-generated method stub
						return o2.getValue().compareTo(o1.getValue());
					}	
		        });
       
		        HashMap<String, Float> sortedMap = new LinkedHashMap<String,Float>();
		        for (Entry<String, Float> entry : list)
		        {
		            sortedMap.put(entry.getKey(), entry.getValue());
		            
		        }
		    	
		        return sortedMap;
		    
		 }
	}	
	
	public static void main(String[] args)throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: CountYelpReview <in> <out>");
			System.exit(2);
		}
		//String classpath=System.getProperty("java.class.path");
		//System.out.println(classpath);
		Job job = new Job(conf,"TopTen");
		job.setJarByClass(TopTen.class);
		
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
