
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
public class AirlineData_V2
{
    private static final Log LOG = LogFactory.getLog(AirlineData_V2.class);
    
    /** Generate a working directory based on the Class name */
    static Path workingPath = new Path("/data/airline_ontime" );
	    
    /** Input files are loaded into here */
    static Path inputPath = new Path(workingPath + "/Input");
    
    /** Default configuration */
    static Configuration config = new Configuration();
    
            
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
				out.append(str[0]).append(",");
				out.append(prepMsg(str[1])).append(",");
				out.append(prepMsg(str[2])).append(",");
				out.append(str[3]).append(",");
				out.append(prepMsg(str[4])).append(",");
				out.append(prepMsg(str[5])).append(",");
				out.append(prepMsg(str[6])).append(",");
				out.append(str[7]).append(",");
				out.append(prepMsg(str[8])).append(",");
				out.append(str[9]).append(",");
				out.append(str[10]);
				context.write(NullWritable.get(), new Text(out.toString()));
						
			}
				
		}
			//LOG.info("Values In For Loop:"+out.toString());
		
		private  String prepMsg(String val)
		{
			String retVal = "null";
			if(val.trim().length() >0 ){
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
        
        //String fileNames1 = "/data/airline_ontime/Input/On_Time_On_Time_Performance_1989_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1991_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2006_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1990_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1997_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2005_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1989_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1999_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1994_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1994_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2007_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1999_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2004_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1990_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2004_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2006_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1997_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1992_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1988_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1998_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1990_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1990_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1997_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1991_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1999_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1996_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2000_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2004_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2002_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2006_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1995_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1997_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2007_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2003_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1991_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1989_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1992_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1988_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1997_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1991_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1999_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1994_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1992_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1989_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1989_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1999_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1994_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1988_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2000_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2004_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1991_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1996_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1997_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1990_4.csv"; 
        
        //String fileNames2 = "/data/airline_ontime/Input/On_Time_On_Time_Performance_2001_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2002_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2004_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1995_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2005_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1991_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1993_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2005_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2001_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1989_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2000_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2007_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2000_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2007_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1995_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1996_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1993_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2006_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1999_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2006_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2004_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1992_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1988_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1988_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2000_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1992_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1996_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1991_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1997_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1990_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2000_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2000_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1996_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1989_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1996_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2004_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2006_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1991_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2003_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1993_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2003_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1997_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2005_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2000_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1996_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2000_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1991_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1988_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2003_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1999_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1998_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1998_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1989_7.csv";
        
        String fileNames3 = "/data/airline_ontime/Input/On_Time_On_Time_Performance_2001_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2002_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1993_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1994_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1995_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2003_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2006_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1988_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1998_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1995_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1999_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1990_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1990_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1993_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1992_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1993_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2005_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2004_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1996_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1995_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2001_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1998_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1994_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2006_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2000_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2003_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1989_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2001_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1999_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1996_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1998_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1990_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1997_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2004_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2005_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2000_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1991_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2003_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1998_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2001_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1992_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2005_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2001_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2004_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2005_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1992_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1995_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1990_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1994_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1995_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2007_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2007_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1991_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2001_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1996_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1989_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1989_5.csv";
        
        String fileNames4 = "/data/airline_ontime/Input/On_Time_On_Time_Performance_1988_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1988_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1988_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1988_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1988_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1989_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1990_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1990_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1991_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1992_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1992_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1992_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1992_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1993_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1993_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1993_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1993_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1993_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1994_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1994_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1994_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1994_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1995_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1995_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1995_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1995_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1996_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1996_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1997_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1997_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1997_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1998_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1998_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1998_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1998_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1998_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1999_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1999_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_1999_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2000_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2001_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2001_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2001_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2001_8.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2002_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2003_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2003_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2003_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2003_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2003_7.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2004_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2004_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2005_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2005_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2005_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2005_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2006_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2006_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2006_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2006_9.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2007_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2007_11.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2007_12.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2007_3.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2007_5.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2007_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_10.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_1.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_2.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_4.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_6.csv,/data/airline_ontime/Input/On_Time_On_Time_Performance_2008_7.csv";
        
                        
        TextInputFormat.setInputPaths(job, fileNames3);
		
		// Set the location where the Reducer will write the output
        TextOutputFormat.setOutputPath(job, new Path(workingPath, "/data/airline_ontime/task1/part3/Output"));
		
		job.setJarByClass(AirlineData_V2.class);
		
		job.setNumReduceTasks(1);
		
		//job.submit();
		
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }

        
}
