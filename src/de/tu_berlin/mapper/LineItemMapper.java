/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.tu_berlin.mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author patte
 */
public class LineItemMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private LongWritable newKey = new LongWritable();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
        String[] line = value.toString().split("\\|");
        String order = line[0];
        Integer quantity = Integer.parseInt(line[4]);
        Double extendedPrice = Double.parseDouble(line[5]);
        Double totalPrice = quantity * extendedPrice;
        word.set(totalPrice.toString() + "|" + quantity);
        newKey = new LongWritable(Long.parseLong(order));
        output.collect(newKey, word);
    }
}
