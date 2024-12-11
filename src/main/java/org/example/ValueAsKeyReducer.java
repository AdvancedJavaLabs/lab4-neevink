package org.example;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ValueAsKeyReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            DoubleWritable dw = new DoubleWritable(-1 * key.get());
            context.write(value, dw);
        }
    }
}

