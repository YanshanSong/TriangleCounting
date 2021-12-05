package improved;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ImprovedReducer2 extends Reducer<Text, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int smallerIdDegree = -1, largerIdDegree = -1;
        for(Text value: values) {
            String info = value.toString();
            String[] data = info.split(",");
            if(data[0].equals("smallerIdDegree")){
                smallerIdDegree = Integer.valueOf(data[1]);
            }else{
                largerIdDegree = Integer.valueOf(data[1]);
            }
        }
        String[] ids = key.toString().split(",");
        int smallerId = Integer.valueOf(ids[0]);
        int largerId = Integer.valueOf(ids[1]);
        if(smallerIdDegree <= largerIdDegree) {
            k.set(String.valueOf(smallerId));
            v.set(String.valueOf(largerId));
        }else{
            k.set(String.valueOf(largerId));
            v.set(String.valueOf(smallerId));
        }
        context.write(k, v);
    }
}
