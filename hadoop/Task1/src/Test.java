import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

public class Test {
	
	
	
	
	
	public static void main(String[] args) 
    {
       System.out.println("Reading File from Java code");
       String str = "19790,DL,990,2008-09-16,ATL,BUF,1510,-5.00,1714,0.00,0,";
       StringBuilder out = new StringBuilder();
       if(str.startsWith("\"") || str.endsWith("\""))
    	   out.append(str.replaceAll("\"", ""));
       
       System.out.println(out);
       
       
       //Name of the file
//       String fileName="C:\\MyCourses\\CloudComputing\\capstone\\data\\samplesRanking\\merge.txt";
//       List<AirportDtls> airportList = new ArrayList<>();
//
//       
//       TreeMap<String, List<AirportDtls>> airportMap = 
//    		   new TreeMap<String, List<AirportDtls>>();
//       AirportDtls apDtls; 
//       
//       try{
//
//          //Create object of FileReader
//          FileReader inputFile = new FileReader(fileName);
//
//          //Instantiate the BufferedReader Class
//          BufferedReader bufferReader = new BufferedReader(inputFile);
//
//          //Variable to hold the one line data
//          String line; 
//          String key = "ATL";
//          
//          // Read file line by line and print on the console
//          while ((line = bufferReader.readLine()) != null)   {
//            System.out.println(line);
//            String[] lineArr = line.split("\t");
//            //String key = lineArr[0].split("-")[0];
//            apDtls = new AirportDtls(lineArr[0], Integer.parseInt(lineArr[1]));
//            airportList.add(apDtls);
//                        
//          }
//          Collections.sort(airportList);
//          airportMap.put(key, airportList.subList(0, 10));
//          System.out.println("Sorted Map:" +airportMap); 
//          //Close the buffer reader
//          bufferReader.close();
//       }catch(Exception e){
//          System.out.println("Error while reading file line by line:" + e.getMessage());                      
//       }

     }

}
