package naive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NaiveDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "/");
        Configuration conf = new Configuration();

        // job1
        Job job1 = Job.getInstance(conf, "job1");
        job1.setJarByClass(NaiveDriver.class);

        job1.setMapperClass(NaiveMapper1.class);
        job1.setReducerClass(NaiveReducer1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);


        job1.setOutputKeyClass(Text.class);
        job1.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job1, new Path("src/main/input/graph.txt"));
        FileOutputFormat.setOutputPath(job1, new Path("src/main/naive_output1"));

        ControlledJob ctrlJob1 = new ControlledJob(conf);
        ctrlJob1.setJob(job1);

        // job2
        Job job2 = Job.getInstance(conf, "job2");
        job1.setJarByClass(NaiveDriver.class);

        job2.setMapperClass(NaiveMapper2.class);
        job2.setReducerClass(NaiveReducer2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputKeyClass(IntWritable.class);

        FileInputFormat.setInputPaths(job2, new Path("src/main/naive_output1"));
        FileOutputFormat.setOutputPath(job2, new Path("src/main/naive_output2"));

        ControlledJob ctrlJob2 = new ControlledJob(conf);
        ctrlJob2.setJob(job2);

        ctrlJob2.addDependingJob(ctrlJob1);

        // main job controller
        JobControl jobCtrl = new JobControl("jobCtrl");
        jobCtrl.addJob(ctrlJob1);
        jobCtrl.addJob(ctrlJob2);

        Thread thread = new Thread(jobCtrl);
        thread.start();
        while (true) {
            if (jobCtrl.allFinished()) {
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }
    }
}
