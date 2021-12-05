package improved;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ImprovedMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if(!line.equals("")) {
            String[] edges = line.split("\\s+");
            if(edges.length == 2) {
                k.set(edges[0]);
                v.set(edges[1]);
                context.write(k, v);
            }
        }
    }
}
