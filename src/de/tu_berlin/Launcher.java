/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.tu_berlin;

import de.tu_berlin.mapper.CustomerMapper;
import de.tu_berlin.mapper.LineItemMapper;
import de.tu_berlin.mapper.OrderMapper;
import de.tu_berlin.reducer.CustomerReducer;
import de.tu_berlin.reducer.LineItemReducer;
import de.tu_berlin.reducer.OrderReducer;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

public class Example {

    public static void main(String[] args) throws Exception {
        System.out.print("Starting EXample");
        Path output = new Path("temp");
        cleanUp(output);
        Double borderValue = 5000000.0;
        JobConf conf = new JobConf(Example.class);
        conf.setJobName("example");
        conf.set("borderValue", borderValue.toString());
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "|");
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(LineItemMapper.class);
        conf.setReducerClass(LineItemReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path("/home/patte/table/lineitem.tbl"));
        FileOutputFormat.setOutputPath(conf, output);

        Path output1 = new Path("temp1");
        cleanUp(output1);

        JobConf custConf = new JobConf(Example.class);
        custConf.setJobName("custMapper");
        custConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "|");
        custConf.set("mapred.textoutputformat.separator", "|");
        custConf.setOutputKeyClass(LongWritable.class);
        custConf.setOutputValueClass(Text.class);

        custConf.setMapperClass(CustomerMapper.class);
        custConf.setReducerClass(CustomerReducer.class);

        custConf.setInputFormat(TextInputFormat.class);
        custConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(custConf, new Path("/home/patte/table/customer.tbl"), new Path("/home/patte/table/orders.tbl"));
        FileOutputFormat.setOutputPath(custConf, output1);

        Path output2 = new Path("output");
        cleanUp(output2);

        JobConf job = new JobConf(Example.class);
        job.setJobName("example2");
        job.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "|");
        job.set("mapred.textoutputformat.separator", "|");
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, output, output1);
        FileOutputFormat.setOutputPath(job, output2);

        Job job1 = new Job(conf);
        Job job2 = new Job(job);
        Job job3 = new Job(custConf);

        JobControl jc = new JobControl("JC");
        jc.addJob(job1);
        jc.addJob(job2);
        jc.addJob(job3);
        job2.addDependingJob(job1);
        job2.addDependingJob(job3);
        jc.run();

//        JobClient.runJob(conf);
//        JobClient.runJob(job);

//        ChainMapper.addMapper(conf, OrderMapper.class,  LongWritable.class, Text.class,
//   Text.class, Text.class, true, conf);
    }

    private static void cleanUp(Path output) {
        try {
            File f = new File(output.toUri() + "/");
            if (f.exists() && f.isDirectory()) {
                FileUtils.deleteDirectory(f);
            }
        } catch (IOException ex) {
            Logger.getLogger(Launcher.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
