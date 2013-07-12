/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.tu_berlin.mapper;

import de.tu_berlin.reducer.LineItemReducer;
import de.tu_berlin.reducer.OrderReducer;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author patte
 */
public class CustomerMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, LongWritable, Text> {

    public final static String CUSTOMER_PREFIX = "CUSTOMR_";
    private Text word;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
        String[] line = value.toString().split("\\|");
        word = new Text();
        if (line.length == 8) {
            //Customer.tbl
            Long newkey = Long.parseLong(line[0]);
            word.set(CUSTOMER_PREFIX + line[1]);
            key.set(newkey);
            output.collect(key, word);
        } else {
            //order table
            Long newkey = Long.parseLong(line[1]);
            word.set(OrderMapper.ORDER_PREFIX + line[0]);
            key.set(newkey);
            output.collect(key, word);
            
            
//            String customerId = "0";
//            int offset = 0;
//            for (int i = 0; i < line.length; i++) {
//                String item = line[i];
//                if (item.startsWith(OrderMapper.ORDER_PREFIX)) {
//                    customerId = item.replace(OrderMapper.ORDER_PREFIX, "");
//                    offset = i;
//                }
//            }
//            Long newkey = Long.parseLong(customerId);
//            word.set(OrderMapper.ORDER_PREFIX + line[0] + "|" + line[(offset == 1) ? 2 : 1] + "|" + line[(offset == 1) ? 3 : 2]);
//            key.set(newkey);
//            output.collect(key, word);
        }
    }
}
