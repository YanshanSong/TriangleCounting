package naive;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NaiveMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.equals("")) {
            String[] components = line.split("\t");
            if(components.length == 2) {
                String originalKeys = components[0];
                String originalValue = components[1];

                k.set(originalKeys);
                v.set(originalValue);
                context.write(k, v);

                String[] keys = originalKeys.split(",");
                // <(v1, u), $>
                k.set(keys[0] + "," + originalValue);
                v.set("$");
                context.write(k, v);

                // <(v2, u), $>
                k.set(keys[1] + "," + originalValue);
                v.set("$");
                context.write(k, v);
            }
        }
    }
}
