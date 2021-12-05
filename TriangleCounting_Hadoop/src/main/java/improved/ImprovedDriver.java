package improved;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ImprovedDriver {

    public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "/");
        Configuration conf = new Configuration();

        // job1
        Job job1 = Job.getInstance(conf, "job1");
        job1.setJarByClass(ImprovedDriver.class);

        job1.setMapperClass(ImprovedMapper1.class);
        job1.setReducerClass(ImprovedReducer1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job1, new Path("src/main/input/graph.txt"));
        FileOutputFormat.setOutputPath(job1, new Path("src/main/improved_output1"));

        ControlledJob ctrlJob1 = new ControlledJob(conf);
        ctrlJob1.setJob(job1);

        // job3
        Job job2 = Job.getInstance(conf, "job2");
        job2.setJarByClass(ImprovedDriver.class);

        job2.setMapperClass(ImprovedMapper2.class);
        job2.setReducerClass(ImprovedReducer2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job2, new Path("src/main/improved_output1"));
        FileOutputFormat.setOutputPath(job2, new Path("src/main/improved_output2"));

        ControlledJob ctrlJob2 = new ControlledJob(conf);
        ctrlJob2.setJob(job2);

        ctrlJob2.addDependingJob(ctrlJob1);

        // job3
        Job job3 = Job.getInstance(conf, "job3");
        job3.setJarByClass(ImprovedDriver.class);

        job3.setMapperClass(ImprovedMapper3.class);
        job3.setReducerClass(ImprovedReducer3.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job3, new Path("src/main/improved_output2"));
        FileOutputFormat.setOutputPath(job3, new Path("src/main/improved_output3"));

        ControlledJob ctrlJob3 = new ControlledJob(conf);
        ctrlJob3.setJob(job3);

        ctrlJob3.addDependingJob(ctrlJob2);

        // job4
        Job job4 = Job.getInstance(conf, "job4");
        job4.setJarByClass(ImprovedDriver.class);

        job4.setMapperClass(ImprovedMapper4.class);
        job4.setReducerClass(ImprovedReducer4.class);

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job4, new Path("src/main/improved_output3"));
        FileOutputFormat.setOutputPath(job4, new Path("src/main/improved_output4"));

        ControlledJob ctrlJob4 = new ControlledJob(conf);
        ctrlJob4.setJob(job4);

        ctrlJob4.addDependingJob(ctrlJob3);

        // main job controller
        JobControl jobCtrl = new JobControl("jobCtrl");
        jobCtrl.addJob(ctrlJob1);
        jobCtrl.addJob(ctrlJob2);
        jobCtrl.addJob(ctrlJob3);
        jobCtrl.addJob(ctrlJob4);

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
