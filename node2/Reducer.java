import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;
import java.io.*;
import java.util.*;


public class Reducer extends Configured implements Tool {

    public static final String FS_PARAM_NAME = "fs.defaultFS";

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        //System.out.println("configured filesystem = " + conf.get(FS_PARAM_NAME));

        FileSystem fs = FileSystem.get(conf);

        //Params
        int numberOfMappers = Integer.parseInt(args[0]);
        String inputName = args[1];
        String outputName = args[2];
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		
		int i;
		Path mapperOutputFilePath;
		String tempName;
		
		FSDataInputStream in;
		byte [] out;
		String outputStr;
		String[] splited;
		int size;
		int splitSize;
		
		for (int k = 0; k < numberOfMappers; k++) {
			tempName = inputName + k + ".txt";
			mapperOutputFilePath = new Path(fs.getHomeDirectory(), tempName);
			
			if (!fs.exists(mapperOutputFilePath)) {
                System.err.println("Mapper Output File missing");
                return 1;
        	}
        	
        	in = fs.open(mapperOutputFilePath);
        	
        	size = in.available();
			
			out = new byte[size];
			in.readFully(0, out, 0, size);
			
			outputStr = new String (out, "UTF-8");
			
			splited = outputStr.split("\\s+");
			
			splitSize = splited.length;
			
			int curVal = 0;
			int asciiAt;
			int ascii0 = (int)'0';
			
			for (i = 0; i < splitSize; i += 2) {
				curVal = 0;
			
				for (int j = 1; j < splited[i+1].length() - 1; j++) {
					asciiAt = (int)splited[i+1].charAt(j);
		
					curVal = curVal * 10 + (asciiAt - ascii0);
				}
				if (map.containsKey(splited[i])) {
					map.put(splited[i], map.get(splited[i]) + curVal);
				}else {
					map.put(splited[i], curVal);
				}
				
			}			
		}
       
		Path reducerOutputPath = new Path(fs.getHomeDirectory(), outputName);
		final FSDataOutputStream streamWriter = fs.create(reducerOutputPath);
	
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
				streamWriter.writeChars(entry.getKey() + " " + entry.getValue()+"\n");
		}
		
		/*for (i = 0; i < numberOfMappers; i++) {
			tempName = inputName + 0 + ".txt";
			mapperOutputFilePath = new Path(fs.getHomeDirectory(), tempName);
			fs.delete(mapperOutputFilePath);
		}*/
		
		out = null;
		fs.close();

		return 0;
		}

        public static void main( String[] args ) throws Exception {
                int returnCode = ToolRunner.run(new Reducer(), args);
                System.exit(returnCode);
        }
}
