/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.tu_berlin.reducer;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author patte
 */
public class LineItemReducer extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {

    public final static String LINE_ITEM_PREFIX = "LINEITEM_";
    private Text result = new Text();
    private Double border;
    private static int counter=0;

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        border = Double.parseDouble(job.get("borderValue"));
    }

    @Override
    public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
        Double totalQuantity = new Double(0);
        Double orderPrice = new Double(0);
        while (values.hasNext()) {
            Text val = values.next();
            String[] entry = val.toString().split("\\|");
            Double price = Double.parseDouble(entry[0]);
            Double quantity = Double.parseDouble(entry[1]);

            orderPrice += price;
            totalQuantity += quantity;
        }
       
        if (orderPrice.doubleValue() > border.doubleValue()) {
            result.set(LINE_ITEM_PREFIX + orderPrice.toString() + "|" + LINE_ITEM_PREFIX + totalQuantity);
            output.collect(key, result);
        }
    }
}
