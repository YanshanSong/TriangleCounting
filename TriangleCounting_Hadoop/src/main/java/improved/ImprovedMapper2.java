package improved;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ImprovedMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.equals("")) {
            String[] components = line.split("\\s+");
            if(components.length == 2) {
                k.set(components[0]);
                v.set(components[1]);
                context.write(k, v);
            }
        }
    }
}
