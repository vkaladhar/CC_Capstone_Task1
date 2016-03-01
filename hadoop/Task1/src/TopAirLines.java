import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TopAirLines {
	
	private static final Log LOG = LogFactory.getLog(TopAirLines.class);
	
	static final String cvsSplitBy = ",";
	
	


	public static class AirlineCountMap extends
			Mapper<Object, Text, Text, IntWritable> {
		

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String airLineCode = "";
						
			String[] values = value.toString().split(cvsSplitBy);
			
						
			if (values[9].matches("-?\\d+(\\.\\d+)?"))
				
				if(Double.parseDouble(values[9]) >-1 && Double.parseDouble(values[9]) < 1)
					//LOG.info("Values:"+values[0]+","+values[8]+","+values[9]);
					if(values[1] != null && values[1].trim().length() > 0){ //Airline
						airLineCode = values[1];
						context.write(new Text(airLineCode), new IntWritable(1));
					}
		}
			
	}

	public static class AirlineCountReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			//LOG.info("Key:"+key);
			//LOG.info("values:"+values);
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class TopAirlinesMap extends
			Mapper<Text, Text, NullWritable, Text> {
		private TreeMap<Integer, String> airlineMap = new TreeMap<Integer, String>();

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			
			LOG.info("SecondMap Key:"+key);
			LOG.info("SecondMap value:"+value);
			Integer count = Integer.parseInt(value.toString());
			String airportCode = key.toString();
			airlineMap.put(count, airportCode);
			
			if (airlineMap.size() > 10) {
				airlineMap.remove(airlineMap.firstKey());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
				
			for(Map.Entry<Integer,String> entry : airlineMap.entrySet()) {
					Integer key = entry.getKey();
					String value = entry.getValue();
					String airLineCntAndCode = key+"\t"+value;
					LOG.info("cleanup value:"+airLineCntAndCode);
					context.write(NullWritable.get(), new Text(airLineCntAndCode));
				  
				}
		}
	}

	public static class TopAirLinesReduce extends
			Reducer<NullWritable, Text, NullWritable, Text> {
		private TreeMap<Integer, String> airlineMap = new TreeMap<Integer, String>();

		@Override
		public void reduce(NullWritable key,
				Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//LOG.info("SecondReduce Value:"+values);
			 for (Text value : values) {
				 	LOG.info("SecondReduce Input Value:"+value.toString());
				 	String v[] = value.toString().split("\t");
				 	//int cntIndex = v.length -1;
				 	
				 	Integer count = Integer.parseInt(v[0].trim());
				 	airlineMap.put(count, v[1]);

		            if (airlineMap.size() > 10) {
		            	airlineMap.remove(airlineMap.firstKey());
		            }
		        }

		       for(Map.Entry<Integer,String> entry : airlineMap.entrySet()) {
					Integer key1 = entry.getKey();
					String value1 = entry.getValue();
					String airportCntAndCode = key1+"\t"+value1;
					LOG.info("SecondReduce output:"+airportCntAndCode);
					context.write(NullWritable.get(), new Text(airportCntAndCode));
				  
				}
		        
		    }
			
	
			
//			for (TextArrayWritable val : values) {
//				Text[] pair = (Text[]) val.toArray();
//				String word = pair[0].toString();
//				Integer count = Integer.parseInt(pair[1].toString());
//				countToWordMap.add(new Pair<Integer, String>(count, word));
//				if (countToWordMap.size() > 10) {
//					countToWordMap.remove(countToWordMap.first());
//				}
//			}
//			for (Pair<Integer, String> item : countToWordMap) {
//				Text word = new Text(item.second);
//				IntWritable value = new IntWritable(item.first);
//				context.write(word, value);
//			}
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path tmpPath = new Path("/data/airline_ontime/cleanedData/tmp");
		fs.delete(tmpPath, false);
		
		Job jobA = Job.getInstance(conf, "airport count");
		jobA.setOutputKeyClass(Text.class);
		jobA.setOutputValueClass(IntWritable.class);
		
		jobA.setMapperClass(AirlineCountMap.class);
		jobA.setReducerClass(AirlineCountReduce.class);
		
		String filename = "/data/airline_ontime/cleanedData/Input/part1.csv,/data/airline_ontime/cleanedData/Input/part2.csv,/data/airline_ontime/cleanedData/Input/part3.csv,/data/airline_ontime/cleanedData/Input/part4.csv";
		
		TextInputFormat.setInputPaths(jobA, filename);
		//TextOutputFormat.setOutputPath(jobA, new Path("/data/airline_ontime/cleanedData/Output"));
		TextOutputFormat.setOutputPath(jobA, tmpPath);
		
		jobA.setJarByClass(TopAirLines.class);
		jobA.waitForCompletion(true);
		
		//System.exit(jobA.waitForCompletion(true) ? 0 : 1);
		
		
		Job jobB = Job.getInstance(conf, "Top airports");
		jobB.setOutputKeyClass(Text.class);
		jobB.setOutputValueClass(IntWritable.class);
		
		jobB.setMapOutputKeyClass(NullWritable.class);
		jobB.setMapOutputValueClass(Text.class);
		
		jobB.setMapperClass(TopAirlinesMap.class);
		jobB.setReducerClass(TopAirLinesReduce.class);
		
		jobB.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(jobB, tmpPath);
		FileOutputFormat.setOutputPath(jobB, new Path("/data/airline_ontime/cleanedData/Output"));
								
		
		jobB.setInputFormatClass(KeyValueTextInputFormat.class);
		jobB.setOutputFormatClass(TextOutputFormat.class);
		
		jobB.setJarByClass(TopAirLines.class);
		System.exit(jobB.waitForCompletion(true) ? 0 : 1);
		
	}
}
