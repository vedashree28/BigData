import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.List;






public class ReduceSideJoin {


	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable>{

		public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {

			String[] mydata = value.toString().split("::");
			if (mydata.length > 23){
				if("review".compareTo(mydata[22])== 0){
					Float new_Variable=Float.parseFloat(mydata[20].toString());
					//System.out.println("key"+mydata[2]+"value"+new_Variable);
					context.write(new Text(mydata[2]),new FloatWritable(new_Variable));

				}
			}

		}		

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
		}
	}


	//------------------------------------------------

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
			//context.write(new Text(key),new Text(avg.toString()));

		}


		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException {

			Integer topTenReview = 0;
			HashMap<String,Float> sortedMap = sortByComparator(ratingMap);
			for (Entry<String,Float> tempvalue : sortedMap.entrySet()){
				//System.out.println("key "+tempvalue.getKey()+"Value "+ tempvalue.getValue());
				context.write(new Text (tempvalue.getKey()),new Text(tempvalue.getValue().toString()));
				topTenReview++;
				/*if(topTenReview>=50)
					break;*/
			}

		}

	}

	private static HashMap<String, Float> sortByComparator(HashMap<String, Float> unsortMap){

		System.out.println("unsorted value");		 	
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
	
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("\t");
			//System.out.println(mydata[0]+ " "+mydata[1]);
			context.write(new Text(mydata[0]), new Text("A"+mydata[1]));
		}
		
	}
	
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("::");
			//System.out.println("id"+mydata[0]+"key"+mydata[2]+"val"+mydata[3]+ " "+mydata[10]);
			context.write(new Text(mydata[2]),new Text("B"+mydata[3]+" "+mydata[10]));

		}
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {

		}
	}

	
	
	public static class Reduce1 extends Reducer<Text,Text,Text,Text> {
		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();
		public Text keyText ;
		
		private  static  TreeMap<Text,Text> completeMap = new TreeMap(Collections.reverseOrder());
		
		Text temp = new Text();
		
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
			keyText= key;
			
			listA.clear();
			
			listB.clear();
			while(values.iterator().hasNext()){
				
				temp = values.iterator().next();
				if(temp.charAt(0)=='A'){
					listA.add(new Text(temp.toString().substring(1)));
				}
				
				else if(temp.charAt(0)=='B'){
					listB.add(new Text(temp.toString().substring(1)));
				}				
			}
			
				executeJoinLogic(context);
			
		}
		
		public void executeJoinLogic(Context context) throws IOException, InterruptedException{ 
	        if (!listA.isEmpty() && !listB.isEmpty()) 
	            for (Text a : listA){ 
	                for (Text b: listB){ 
	                	//System.out.println("key"+b+" value"+a);
	                    //context.write(b, a); 
	                    completeMap.put(new Text(keyText+" "+b), a);
	                } 
	            } 
	    } 
		
		protected void cleanup(Context context)throws IOException, InterruptedException {
			
			Integer topTenReview = 0;
			HashMap<Text,Text> sortedMap = sortByComparator(completeMap);
			for (Entry<Text,Text> tempvalue : sortedMap.entrySet()){
				context.write(new Text (tempvalue.getKey()),new Text(tempvalue.getValue().toString()));
				topTenReview++;
				if(topTenReview>=10)
					break;
			}
			
		}
		
		
		private static HashMap<Text, Text> sortByComparator(TreeMap<Text, Text> unsortMap)
	    {

	        LinkedList<Entry<Text,Text>> list = new LinkedList<Entry<Text,Text>>(unsortMap.entrySet());

	        // Sorting the list based on values
	        Collections.sort(list, new Comparator<Entry<Text, Text>>()
	        {
				@Override
				public int compare(Entry<Text, Text> o1,
						Entry<Text, Text> o2) {
					// TODO Auto-generated method stub
					return o2.getValue().compareTo(o1.getValue());
				}

			
	        });
	        // Maintaining insertion order with the help of LinkedList
	        HashMap<Text, Text> sortedMap = new LinkedHashMap<Text,Text>();
	        for (Entry<Text, Text> entry : list)
	        {
	            sortedMap.put(entry.getKey(), entry.getValue());
	        }

	        return sortedMap;
	    }
   

	}

	public static void main(String[] args)throws Exception {
		
		/*
		 * 
		 * Job 1
		 */
		
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();		// get all args
		if (otherArgs.length != 4) {
			System.err.println("Usage: CountYelpReview <in> <out>");
			System.exit(2);
		}
		//String classpath=System.getProperty("java.class.path");
		//System.out.println(classpath);
		
		Job job = new Job(conf,"TopTen");
		job.setJarByClass(ReduceSideJoin.class);
		
		
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		job.waitForCompletion(true);
		/*
		 * Job 2
		 */
		System.out.println("Job Success "+job.isSuccessful());
		
		Job job1 = new Job(conf,"Job2");
		job1.setJarByClass(ReduceSideJoin.class);
		
		job1.setMapperClass(Map1.class);		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setMapperClass(Map2.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setReducerClass(Reduce1.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		System.out.println("Argument 1"+args[1]);
		System.out.println("Argument 2"+args[2]);
		
		MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class ,Map1.class );
		MultipleInputs.addInputPath(job1, new Path(args[1]),TextInputFormat.class,Map2.class );
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[3]));

		System.exit(job1.waitForCompletion(true) ? 0 : 1);	

	}

}