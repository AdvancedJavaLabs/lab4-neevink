package org.example.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ValueAsKeyReducer extends Reducer<DoubleWritable, ValueAsKeyData, Text, Text> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<ValueAsKeyData> values, Context context) throws IOException, InterruptedException {
        for (ValueAsKeyData value : values) {
            Text category = new Text(value.getCategory());
            context.write(category, new Text(String.format("%.2f\t%d", -1 * key.get(), value.getQuantity())));
        }
    }
}

