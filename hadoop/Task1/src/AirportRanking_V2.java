import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AirportRanking_V2 {

	private static final Log LOG = LogFactory.getLog(AirportRanking_V2.class);

	static final String cvsSplitBy = ",";

	public static class AirportOrigDestMap extends
			Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] values = value.toString().split(cvsSplitBy);
			
			if (values[7].matches("-?\\d+(\\.\\d+)?"))
				if (Double.parseDouble(values[7]) >= -1
						&& Double.parseDouble(values[7]) <= 1)
					context.write(new Text(values[4]), new Text(values[5]));

		}
	}

	public static class AirportOrigDestReduce extends
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			
			StringBuilder destAirports = new StringBuilder();
			for (Text val : values) {
				destAirports.append(val.toString()).append(",");
			}
			
			context.write(key, new Text(destAirports.toString()));
		}
	}

	public static class AirportOrigDestCntMap extends
			Mapper<Object, Text, Text, IntWritable> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] airportOrigDest = value.toString().split("\t");
			String airportKey = airportOrigDest[0];
			String airportsValues = airportOrigDest[1];

			String[] values = airportsValues.split(cvsSplitBy);
			
			for (String val : values) {
				String newKey = airportKey + "-" + val;
				context.write(new Text(newKey), new IntWritable(1));
			}

		}
	}

	public static class AirportOrigDestCntReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
		
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	

	
	public static class TopAirportsMap extends Mapper<Text, Text, Text, Text> {
		

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			

			String airportCode = key.toString().split("-")[0];
			String newValue = key.toString() + "\t" + value.toString();
			//context.write(new Text(airportCode), new Text(newValue));
			
			context.write(new Text(airportCode), new Text(newValue));

		}

	}

	public static class TopAirportsReduce extends
			Reducer<Text, Text, Text, Text> {
		
		private AirportDtls apDtls;
		private List<AirportDtls> apDtlsSet;
		private List<AirportDtls> apDtlsSetCopy;
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			apDtlsSet = new ArrayList<>();
		
			for (Text value : values) {
				LOG.info("SecondReduce Input Key:" + key+"\t Value:"+value.toString());
				String v[] = value.toString().split("\t");
				if(v[0].startsWith(key.toString())){
					apDtls = new AirportDtls(v[0], Integer.parseInt(v[1]));
					apDtlsSet.add(apDtls);
					
				}
	
			}
			if(apDtlsSet.size() > 10)
				apDtlsSetCopy = apDtlsSet.subList(0, 10);
			else
				apDtlsSetCopy = apDtlsSet;
			
			//apDtlsList.addAll(apDtlsSet);
			if(apDtlsSetCopy != null)
			Collections.sort(apDtlsSetCopy, AirportDtls.Comparators.CNT);
			
			LOG.info("Sorted AirportList:" + apDtlsSetCopy);
			
			
			for(AirportDtls apdtls1 : apDtlsSetCopy)
			{
				String newVal = apdtls1.getOrigDest().split("-")[1] + "\t" + apdtls1.getCnt();
				context.write(key, new Text(newVal));
			}
			
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// Path tmpPath1 = new Path("/data/airline_ontime/sample/tmp1");
		// Path tmpPath2 = new Path("/data/airline_ontime/sample/tmp2");

		// fs.delete(tmpPath1, true);
		// fs.delete(tmpPath2, true);

		Job jobA = Job.getInstance(conf, "map airports");
		jobA.setOutputKeyClass(Text.class);
		jobA.setOutputValueClass(Text.class);

		jobA.setMapperClass(AirportOrigDestMap.class);
		jobA.setReducerClass(AirportOrigDestReduce.class);

		String filename = "/data/airline_ontime/cleanedData/Input/part1.csv,/data/airline_ontime/cleanedData/Input/part2.csv,/data/airline_ontime/cleanedData/Input/part3.csv,/data/airline_ontime/cleanedData/Input/part4.csv";
		
		//String filename = "/data/airline_ontime/sample/Input/2008_7_xtract.csv";

		TextInputFormat.setInputPaths(jobA, filename.trim());
		// TextOutputFormat.setOutputPath(jobA, new
		// Path("/data/airline_ontime/cleanedData/Output"));
		TextOutputFormat.setOutputPath(jobA, new Path(
				"/data/airline_ontime/cleanedData/grp2_1/tmp1"));

		jobA.setJarByClass(AirportRanking_V2.class);
		jobA.waitForCompletion(true);

		// System.exit(jobA.waitForCompletion(true) ? 0 : 1);

		Job jobB = Job.getInstance(conf, "Map and count all airports");
		jobB.setOutputKeyClass(Text.class);
		jobB.setOutputValueClass(IntWritable.class);

		jobB.setMapOutputKeyClass(Text.class);
		jobB.setMapOutputValueClass(IntWritable.class);

		jobB.setMapperClass(AirportOrigDestCntMap.class);
		jobB.setReducerClass(AirportOrigDestCntReduce.class);
		
		jobB.setNumReduceTasks(1);

		TextInputFormat.setInputPaths(jobB, "/data/airline_ontime/cleanedData/grp2_1/tmp1");
		TextOutputFormat.setOutputPath(jobB, new Path(
				"/data/airline_ontime/cleanedData/grp2_1/tmp2"));

		jobB.setJarByClass(AirportRanking_V2.class);
		jobB.waitForCompletion(true);

		Job jobC = Job.getInstance(conf, "Top airports");
		jobC.setOutputKeyClass(Text.class);
		jobC.setOutputValueClass(IntWritable.class);

		jobC.setMapOutputKeyClass(Text.class);
		jobC.setMapOutputValueClass(Text.class);

		jobC.setMapperClass(TopAirportsMap.class);
		jobC.setReducerClass(TopAirportsReduce.class);

		jobC.setNumReduceTasks(1);

		TextInputFormat.setInputPaths(jobC, "/data/airline_ontime/cleanedData/grp2_1/tmp2");
		TextOutputFormat.setOutputPath(jobC, new Path(
				"/data/airline_ontime/cleanedData/grp2_1/Output"));

		jobC.setInputFormatClass(KeyValueTextInputFormat.class);
		jobC.setOutputFormatClass(TextOutputFormat.class);

		jobC.setJarByClass(AirportRanking_V2.class);

		System.exit(jobC.waitForCompletion(true) ? 0 : 1);
	}
}

