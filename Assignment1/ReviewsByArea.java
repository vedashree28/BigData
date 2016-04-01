import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ReviewsByArea {
	
	public static class ReviewsByAreaMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		HashMap<String, String> businessLocationMap;
		
		@SuppressWarnings("deprecation")
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			businessLocationMap = new HashMap<String, String>();
			Path[] localCachePaths = context.getLocalCacheFiles();
			for(Path cacheFile : localCachePaths){
				String line = null;
				String fileName = cacheFile.getName();
				File file = new File(fileName);
				FileReader fileReader = new FileReader(file);
				BufferedReader br = new BufferedReader(fileReader);
				while((line=br.readLine())!=null){
					String[] input = line.trim().split("::");
					businessLocationMap.put(input[2].trim(), input[3].trim());
				}
				br.close();
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] input = value.toString().trim().split("::");
			try{
				if(businessLocationMap.get(input[2].trim()).contains("Stanford")){
					context.write(new Text(input[8].trim()), new Text(input[1].trim()));
				}
			}
			catch(NullPointerException e){
				//don't emit anything
				System.err.println(e);
			}
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException{
		Configuration conf=new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length!=3){
			System.err.println("Error! Insufficient arguments. Provide arguments <Input file path:businesses> <input directory:reviews> <Output directory>");
			System.exit(2);
		}
		Job job=new Job(conf, "ReviewsByArea");
		job.setJarByClass(ReviewsByArea.class);
		job.setMapperClass(ReviewsByAreaMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.addCacheFile(new URI(otherArgs[0]));
		FileInputFormat.setInputPaths(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}