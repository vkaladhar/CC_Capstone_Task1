import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

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

public class TopAirports {
	
	private static final Log LOG = LogFactory.getLog(TopAirports.class);
	
	static final String cvsSplitBy = ",";
	
	
//	public static class TextArrayWritable extends ArrayWritable {
//		public TextArrayWritable() {
//			super(Text.class);
//		}
//
//		public TextArrayWritable(String[] strings) {
//			super(Text.class);
//			Text[] texts = new Text[strings.length];
//			for (int i = 0; i < strings.length; i++) {
//				texts[i] = new Text(strings[i]);
//			}
//			set(texts);
//		}
//	}

	public static class AirportCountMap extends
			Mapper<Object, Text, Text, IntWritable> {
		

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String airportCode = "";
			String line = value.toString();
			
			String[] values = value.toString().split(cvsSplitBy);
			//LOG.info("Values:"+values);
						
			if(values[0] != null && values[0].trim().length() > 0){
				if(values[4] != null && values[4].trim().length() > 0){ //Dept
					if(values[4].length() ==5)
						airportCode = values[4];
				}	context.write(new Text(airportCode), new IntWritable(1));
				if(values[5] != null && values[5].trim().length() > 0){ //Arrival
						if(values[5].length() ==5)
							airportCode = values[5];
						context.write(new Text(airportCode), new IntWritable(1));
				}
				
			}
			
		}
	}

	public static class AirportCountReduce extends
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

	public static class TopAirportsMap extends
			Mapper<Text, Text, NullWritable, Text> {
		private TreeMap<Integer, String> airportMap = new TreeMap<Integer, String>();

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			
			//LOG.info("SecondMap Key:"+key);
			//LOG.info("SecondMap value:"+value);
			Integer count = Integer.parseInt(value.toString());
			String airportCode = key.toString();
			airportMap.put(count, airportCode);
			
			if (airportMap.size() > 10) {
				airportMap.remove(airportMap.firstKey());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
				
			for(Map.Entry<Integer,String> entry : airportMap.entrySet()) {
					Integer key = entry.getKey();
					String value = entry.getValue();
					String airportCntAndCode = key+"\t"+value;
					LOG.info("cleanup value:"+airportCntAndCode);
					context.write(NullWritable.get(), new Text(airportCntAndCode));
				  
				}
			
			

			
//			for (Pair<Integer, String> item : airportMap) {
//				String[] strings = { item.second, item.first.toString() };
//				TextArrayWritable val = new TextArrayWritable(strings);
//				context.write(NullWritable.get(), val);
//			}
		}
	}

	public static class TopAirportsReduce extends
			Reducer<NullWritable, Text, NullWritable, Text> {
		private TreeMap<Integer, String> airportMap = new TreeMap<Integer, String>();

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
		            airportMap.put(count, v[1]);

		            if (airportMap.size() > 10) {
		            	airportMap.remove(airportMap.firstKey());
		            }
		        }

		       for(Map.Entry<Integer,String> entry : airportMap.entrySet()) {
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
		
		jobA.setMapperClass(AirportCountMap.class);
		jobA.setReducerClass(AirportCountReduce.class);
		
		String filename = "/data/airline_ontime/cleanedData/Input/part1.csv,/data/airline_ontime/cleanedData/Input/part2.csv,/data/airline_ontime/cleanedData/Input/part3.csv,/data/airline_ontime/cleanedData/Input/part4.csv";
		
		TextInputFormat.setInputPaths(jobA, filename);
		//TextOutputFormat.setOutputPath(jobA, new Path("/data/airline_ontime/cleanedData/Output"));
		TextOutputFormat.setOutputPath(jobA, tmpPath);
		
		jobA.setJarByClass(TopAirports.class);
		jobA.waitForCompletion(true);
		
		//System.exit(jobA.waitForCompletion(true) ? 0 : 1);
		
		
		Job jobB = Job.getInstance(conf, "Top airports");
		jobB.setOutputKeyClass(Text.class);
		jobB.setOutputValueClass(IntWritable.class);
		
		jobB.setMapOutputKeyClass(NullWritable.class);
		jobB.setMapOutputValueClass(Text.class);
		
		jobB.setMapperClass(TopAirportsMap.class);
		jobB.setReducerClass(TopAirportsReduce.class);
		
		jobB.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(jobB, tmpPath);
		FileOutputFormat.setOutputPath(jobB, new Path("/data/airline_ontime/cleanedData/Output"));
								
		
		jobB.setInputFormatClass(KeyValueTextInputFormat.class);
		jobB.setOutputFormatClass(TextOutputFormat.class);
		
		jobB.setJarByClass(TopAirports.class);
		System.exit(jobB.waitForCompletion(true) ? 0 : 1);
	}
}
/*
class Pair<A extends Comparable<? super A>, B extends Comparable<? super B>>
		implements Comparable<Pair<A, B>> {

	public final A first;
	public final B second;

	public Pair(A first, B second) {
		this.first = first;
		this.second = second;
	}

	public static <A extends Comparable<? super A>, B extends Comparable<? super B>> Pair<A, B> of(
			A first, B second) {
		return new Pair<A, B>(first, second);
	}

	@Override
	public int compareTo(Pair<A, B> o) {
		int cmp = o == null ? 1 : (this.first).compareTo(o.first);
		return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
	}

	@Override
	public int hashCode() {
		return 31 * hashcode(first) + hashcode(second);
	}

	private static int hashcode(Object o) {
		return o == null ? 0 : o.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Pair))
			return false;
		if (this == obj)
			return true;
		return equal(first, ((Pair<?, ?>) obj).first)
				&& equal(second, ((Pair<?, ?>) obj).second);
	}

	private boolean equal(Object o1, Object o2) {
		return o1 == o2 || (o1 != null && o1.equals(o2));
	}

	@Override
	public String toString() {
		return "(" + first + ", " + second + ')';
	}
}
*/