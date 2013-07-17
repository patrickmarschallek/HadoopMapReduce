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

public class Launcher {

    private static String customerFile;
    private static String orderFile;
    private static String itemFile;

    public static void main(String[] args) throws Exception {
        Launcher.handleArguments(args);
        Path output = new Path("temp");
        cleanUp(output);
        Double borderValue = 5000000.0;
        JobConf conf = new JobConf(Launcher.class);
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

        FileInputFormat.setInputPaths(conf, new Path(Launcher.itemFile));
        FileOutputFormat.setOutputPath(conf, output);

        Path output1 = new Path("temp1");
        cleanUp(output1);

        JobConf custConf = new JobConf(Launcher.class);
        custConf.setJobName("custMapper");
        custConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "|");
        custConf.set("mapred.textoutputformat.separator", "|");
        custConf.setOutputKeyClass(LongWritable.class);
        custConf.setOutputValueClass(Text.class);

        custConf.setMapperClass(CustomerMapper.class);
        custConf.setReducerClass(CustomerReducer.class);

        custConf.setInputFormat(TextInputFormat.class);
        custConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(custConf, new Path(Launcher.customerFile), new Path(Launcher.orderFile));
        FileOutputFormat.setOutputPath(custConf, output1);

        Path output2 = new Path("output");
        cleanUp(output2);

        JobConf job = new JobConf(Launcher.class);
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

    private static void handleArguments(String[] args) {
        if (args.length < 4) {
            printHelp();
        } else {
            Launcher.customerFile = args[1];
            Launcher.orderFile = args[2];
            Launcher.itemFile = args[3];
        }
    }

    private static void printHelp() {
        System.out.println("start.sh <1> <2> <3> <4> \n\n");
        System.out.println("1: Schwellwert(x) für den Auftragswert. Es werden alle\n\tAufträger mit einem Auftragswert ueber (x) angezeigt");
        System.out.println("2: Path to the customer.tbl");
        System.out.println("3: Path to the orders.tbl");
        System.out.println("4: Path to the lineitem.tbl");
        System.out.println("");
        System.exit(0);
    }
}
