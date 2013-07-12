/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.tu_berlin.mapper;

import de.tu_berlin.reducer.LineItemReducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author patte
 */
public class OrderMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, LongWritable, Text> {

    public final static String ORDER_PREFIX = "ORDER_";
    private Text word;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
        String[] line = value.toString().split("\\|");
        word = new Text();

        if (line[1].startsWith(LineItemReducer.LINE_ITEM_PREFIX)) {
            //lineItem
            word.set(line[1] + "|" + line[2]);
            Long newkey = Long.parseLong(line[0]);
            key.set(newkey);
            output.collect(key, word);
        } else {
//            OrderCustMap
            List<String> orders = new ArrayList();
            String customerName = "";
            for (String val : line) {
                if (val.startsWith(ORDER_PREFIX)) {
                    orders.add(val.replace(ORDER_PREFIX, ""));
                } else {
                    customerName = val.replace(CustomerMapper.CUSTOMER_PREFIX, "");
                }
            }
            for (String orderKey : orders) {
                word.set(customerName);
                Long newkey = Long.parseLong(orderKey);
                key.set(newkey);
                output.collect(key, word);
            }

        }


//        if (line[1].startsWith(LineItemReducer.LINE_ITEM_PREFIX)) {
//            word.set(line[1] + "|" + line[2]);
//            Long newkey = Long.parseLong(line[0]);
//            key.set(newkey);
//            output.collect(key, word);
//        } else {
//            word.set(ORDER_PREFIX + line[1]);
//            Long newkey = Long.parseLong(line[0]);
//            key.set(newkey);
//            output.collect(key, word);
//        }
    }
}