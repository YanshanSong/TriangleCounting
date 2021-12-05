package improved;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ImprovedReducer1 extends Reducer<Text, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> list = new ArrayList<String>();
        for(Text value: values) {
            list.add(value.toString());
        }
        for(int i = 0; i < list.size(); i++) {
            if(Integer.valueOf(key.toString()) < Integer.valueOf(list.get(i))) {
                k.set(key + "," + list.get(i));
                v.set("smallerIdDegree" + "," + list.size());
            }else{
                k.set(list.get(i) + "," + key);
                v.set("largerIdDegree" + "," + list.size());
            }
            context.write(k, v);
        }
    }
}
