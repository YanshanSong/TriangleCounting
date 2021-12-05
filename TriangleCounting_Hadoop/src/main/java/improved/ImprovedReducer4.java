package improved;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ImprovedReducer4 extends Reducer<Text, Text, Text, IntWritable> {
    private static int res = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean flag = false;
        int edgeCount = 0;
        for(Text value: values) {
            if(!value.toString().equals("$")) {
                edgeCount++;
            }else if(!flag) {
                flag = true;
            }
        }
        if(flag) {
            res += edgeCount;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        context.write(new Text("TriangleCount"), new IntWritable(res));
    }
}
