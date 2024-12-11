package org.example;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ValueAsKeyMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    private DoubleWritable outKey = new DoubleWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length == 3) {
            String originalKey = fields[0];
            double val = Double.parseDouble(fields[1]);
            outKey.set(-1 * val);
            context.write(outKey, new Text(originalKey));
        }
    }
}

