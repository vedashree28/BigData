import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException; 
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser;





public class MapSideJoin {

	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
				
		//private static HashMap<String,String> businessLocMap = new HashMap<String, String>();
		private BufferedReader br;
		private ArrayList<String> BusinessIdList = new ArrayList<String>();
		
		protected void setup(Context context)throws IOException, InterruptedException {
			      
		      super.setup(context);
				
				Path[] localCachePaths = context.getLocalCacheFiles();
				for(Path cacheFile : localCachePaths){
					String line = null;
					String fileName = cacheFile.getName();
					File file = new File(fileName);
					FileReader fileReader = new FileReader(file);
					BufferedReader br = new BufferedReader(fileReader);
					while((line=br.readLine())!=null){
						String[] mydata= line.split("::");		    		
			    		String buisnessid = mydata[2];	
			    		//System.out.println(buisnessid);
			    		if(mydata[4].contains("Stanford")== true)		
			    			BusinessIdList.add(buisnessid);
						
					}
					br.close();
				}
			
		}
		
		
		
		public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] mydata = value.toString().trim().split("::");
			//System.out.println(mydata[8]);
			if(BusinessIdList.contains(mydata[2])){
				context.write(new Text(mydata[8]), new Text(mydata[1]));
			}
			
		}
	
}	
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new URI("hdfs://localhost:54310/user/veda/business.csv"),conf);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length!=3){
			System.err.println("Error! Insufficient arguments. Provide arguments <Input file path:businesses> <input directory:reviews> <Output directory>");
			System.exit(2);
		}
		
		Job job = new Job(conf,"ReviewsByArea");
		job.setJarByClass(MapSideJoin.class);
		job.addCacheFile(new URI(args[0]));
		
		job.setMapperClass(Map1.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[1]));		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
}
