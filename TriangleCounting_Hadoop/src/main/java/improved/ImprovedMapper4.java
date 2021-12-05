package improved;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ImprovedMapper4 extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.equals("")) {
            String[] components = line.split("\\s+");
            if(components.length == 2) {

                String originalKey = components[0];
                String originalValues = components[1];

                String[] values = originalValues.split(",");
                if(!values[1].equals("null")) {

                    // <v; <u, w>> --> <<u, w>; v>
                    k.set(originalValues);
                    v.set(originalKey);
                    context.write(k, v);
                }else {
                    //  <v; <u, null>> --> <<u, v>, $>
                    k.set(originalKey + "," + values[0]);
                    v.set("$");
                    context.write(k, v);
                }
            }
        }
    }
}
