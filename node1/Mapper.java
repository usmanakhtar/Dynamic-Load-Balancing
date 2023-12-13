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

public class Mapper extends Configured implements Tool {

    public static final String FS_PARAM_NAME = "fs.defaultFS";

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        //System.out.println("configured filesystem = " + conf.get(FS_PARAM_NAME));

        FileSystem fs = FileSystem.get(conf);

        //Params
        int offset = Integer.parseInt(args[0]);
        int size = Integer.parseInt(args[1]);
        String inputName = args[2];
        String outputName = args[3];


        Path dataset = new Path(fs.getHomeDirectory(), inputName);

        if (!fs.exists(dataset)) {
                System.err.println("output path doesn't exist");
                return 1;
        }

        FSDataInputStream in = fs.open(dataset);


        byte [] out = new byte[size];

        int numBytes;

	in.readFully(offset, out, 0, size);

    String outputStr = new String (out, "UTF-8");

	String[] splited = outputStr.split("\\s+");

	int splitSize = splited.length;
	int i;

	Map<String, Integer> map = new HashMap<String, Integer>();

	for (i = 0; i < splitSize; i++) {
		if (splited[i].length() > 2)
			if (map.containsKey(splited[i])) {
				map.put(splited[i], map.get(splited[i])+1);
			}else {
				map.put(splited[i], 1);
			}
		//System.out.println(splited[i]);
	}

	Path path = new Path(fs.getHomeDirectory(), outputName);
	final FSDataOutputStream streamWriter = fs.create(path);

	for (Map.Entry<String, Integer> entry : map.entrySet()) {
    		streamWriter.writeChars(entry.getKey() + " " + entry.getValue()+"\n");
	}

        out = null;
        fs.close();

        return 0;
    }

        public static void main( String[] args ) throws Exception {
                int returnCode = ToolRunner.run(new Mapper(), args);
                System.exit(returnCode);
        }
}
