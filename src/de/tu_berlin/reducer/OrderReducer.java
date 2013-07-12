/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.tu_berlin.reducer;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author patte
 */
public class OrderReducer extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {

    private Text result = new Text();

    @Override
    public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
        String resultLine = "";
        boolean containItems = false;

        while (values.hasNext()) {
            Text val = values.next();
            if (val.find(LineItemReducer.LINE_ITEM_PREFIX) != -1) {
                containItems = true;
            }
            resultLine += val.toString().replace(LineItemReducer.LINE_ITEM_PREFIX, "") + "|";
        }
        resultLine += resultLine.substring(0, resultLine.length() - 1);
       
        if (containItems) { 
            this.result.set(resultLine);
            output.collect(key, result);
        }
    }
}
