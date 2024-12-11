package org.example;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingIntComparator extends WritableComparator {
    protected DescendingIntComparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        IntWritable key1 = (IntWritable) a;
        IntWritable key2 = (IntWritable) b;
        return -1 * key1.compareTo(key2);
    }
}

