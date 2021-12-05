package naive;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NaiveReducer1 extends Reducer<Text, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> list = new ArrayList<String>();
        for(Text value: values) {
            list.add(value.toString());
        }
        for(int i = 0; i < list.size(); i++) {
            for (int j = 0; j < list.size(); j++) {
                if (Integer.valueOf(list.get(i)) < Integer.valueOf(list.get(j))) {
                    k.set(list.get(i) + "," + list.get(j));
                    v.set(key);
                    context.write(k, v);
                }
            }
        }
    }
}
