
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 */
public class AirlineData_2008
{
    private static final Log LOG = LogFactory.getLog(AirlineData_2008.class);
    
    /** Generate a working directory based on the Class name */
    static Path workingPath = new Path("/data/airline_ontime" );
	    
    /** Input files are loaded into here */
    static Path inputPath = new Path(workingPath + "/Input");
    
    /** Default configuration */
    static Configuration config = new Configuration();
    
    //static String[] grp3Inputs = {"CMI","ORD","LAX","JAX","DFW","CRP","SLC","BFL","SFO","PHX","JFK"};
	        
    /**
     * This Mapper class checks the filename ends with the .csv extension, cleans
     * the text and then applies the simple WordCount algorithm.
     *
     */
    public static class MyMapper
        extends Mapper<LongWritable, Text, NullWritable, Text>
    {
        //private final static LongWritable one = new LongWritable( 1 );
        private Text word = new Text();
		String[] keys = null;
		String cvsSplitBy = ",";
		
		@Override
        public void map( LongWritable key, Text value, Context context )
            throws IOException, InterruptedException
        {
			String[] values = value.toString().split(cvsSplitBy);
			StringBuilder out = new StringBuilder();
			
			out.append(prepMsg(values[7])).append(","); //AirlineID
			out.append(prepMsg(values[8])).append(",");//Carrier
			out.append(prepMsg(values[10])).append(",");//FlightNum
			out.append(prepMsg(values[5])).append(",");//FlightDate
			out.append(prepMsg(values[11])).append(",");//Origin
			out.append(prepMsg(values[18])).append(",");//Dest
			out.append(prepMsg(values[25])).append(",");//DepTime
			out.append(prepMsg(values[27])).append(",");//DepDelayMinutes
			out.append(prepMsg(values[36])).append(",");//ArrTime
			out.append(prepMsg(values[38])).append(",");//ArrDelayMinutes
			out.append(prepMsg(values[41]));//Cancelled
			context.write(NullWritable.get(), new Text(out.toString()));
			
        }
		
		private String prepMsg(String val)
		{
			if(val !=null && val.trim().length() >0 )
				return val;
			else
				return "null";
		}
		
    }
    
    /**
     * Reducer for the ZipFile test, identical to the standard WordCount example
     */
    public static class MyReducer
        extends Reducer<NullWritable, Text, NullWritable, Text>
    {
    	
		@Override
		public void reduce( NullWritable key, Iterable<Text> values, Context context )
            throws IOException, InterruptedException
        {
			LOG.info("Processing Reduce tasks Values:"+values);
			
            //List<String> grp3List = Arrays.asList(grp3Inputs);  
            
			
			
			for (Text val : values) {
				StringBuilder out = new StringBuilder();
				String str[] = val.toString().split(",");
				if(!str[0].contains("\"AirlineID\""))
					if(!str[10].contains("null"))
						if (str[7].matches("-?\\d+(\\.\\d+)?") && str[9].matches("-?\\d+(\\.\\d+)?")){
							if ( Double.parseDouble(str[7]) >= -30 && Double.parseDouble(str[9]) >= -30){
								if ( Double.parseDouble(str[7]) <= 30 &&  Double.parseDouble(str[9]) <= 30){
									//if(grp3List.contains(prepMsg(str[4])) && grp3List.contains(prepMsg(str[5])) ){
		//								out.append(prepMsg(str[3])).append(",");
		//								out.append(prepMsg(str[4])).append(",");
		//								out.append(prepMsg(str[5])).append(",");
		//								out.append(prepMsg(str[6])).append(",");
		//								out.append(prepMsg(str[7])).append(",");
		//								out.append(prepMsg(str[8])).append(",");
		//								out.append(prepMsg(str[9])).append(",");
		//								out.append(prepMsg(str[10]));
										
										out.append(prepMsg(str[3])).append(",");
					        			out.append(prepMsg(str[4])).append("-").append(prepMsg(str[5])).append(",");
					        			out.append(prepMsg(str[1])).append("-").append(prepMsg(str[2])).append(",");
					        			out.append(prepMsg(str[6])).append(",");
					        			out.append(prepMsg(str[8]));
										context.write(NullWritable.get(), new Text(out.toString()));
									//}
							
								}
								
							}
						}
				
			}
			//LOG.info("Values In For Loop:"+out.toString());
			
			
        }
		
		private String prepMsg(String val)
		{
			String retVal = "null";
			if(val !=null && val.trim().length() >0 ){
				if(val.startsWith("\"") || val.endsWith("\""))
					retVal = val.replaceAll("\"", "");
				else
					retVal = val;
			}
						
			return retVal;
		}
		
    }

    /**
     * This test operates on a single file
     * 
     * Expected result: success
     * 
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public static void main(String arggs[])
			throws IOException, ClassNotFoundException, InterruptedException
    {
        LOG.info( "============================================================" );
        LOG.info( "==                Running testSingle()                    ==" );
        LOG.info( "============================================================" );
        
      		
		Job job = Job.getInstance(config,"AirlineData_2008");
      		
		// Set the output Key type for the Mapper
		job.setMapOutputKeyClass(NullWritable.class);
 
		// Set the output Value type for the Mapper
		job.setMapOutputValueClass(Text.class);
		
		// The output files will contain "Word [TAB] Count"
        job.setOutputKeyClass(Text.class);
		
        job.setOutputValueClass(Text.class);
		
        // Set the Mapper Class
        job.setMapperClass(MyMapper.class);
		
		//job.setCombinerClass(MyReducer.class);
		// Set the Reducer Class
        job.setReducerClass(MyReducer.class);
        
        // Set the format of the input that will be provided to the program 
        job.setInputFormatClass(TextInputFormat.class);
		// Set the format of the output for the program
        job.setOutputFormatClass(TextOutputFormat.class);
                        
        String fileNames = "/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_10.csv";
            
		TextInputFormat.setInputPaths(job, fileNames);
		
		// Set the location where the Reducer will write the output
        TextOutputFormat.setOutputPath(job, new Path(workingPath, "/grp3_2/Output"));
		
		job.setJarByClass(AirlineData_2008.class);
		
		job.setNumReduceTasks(1);
		
		//job.submit();
		
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }

        
}
