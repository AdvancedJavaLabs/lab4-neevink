package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.io.IntWritable;

public class SalesAnalysisJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: SalesPipelineJob <input path> <intermediate output path> <final output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // Первая задача: Анализ продаж
        Job salesAnalysisJob = Job.getInstance(conf, "sales analysis");
        salesAnalysisJob.setJarByClass(SalesAnalysisJob.class);
        salesAnalysisJob.setMapperClass(SalesMapper.class);
        salesAnalysisJob.setReducerClass(SalesReducer.class);
        salesAnalysisJob.setMapOutputKeyClass(Text.class);
        salesAnalysisJob.setMapOutputValueClass(SalesData.class);
        salesAnalysisJob.setOutputKeyClass(Text.class);
        salesAnalysisJob.setOutputValueClass(Text.class);
//        salesAnalysisJob.setSortComparatorClass(CustomKeyComparator.class);
//        salesAnalysisJob.setOutputFormatClass(CustomTextOutputFormat.class);

        FileInputFormat.addInputPath(salesAnalysisJob, new Path(args[0]));
        Path intermediateOutput = new Path(args[1]);
        FileOutputFormat.setOutputPath(salesAnalysisJob, intermediateOutput);

        boolean success = salesAnalysisJob.waitForCompletion(true);

        if (!success) {
            System.exit(1);
        }

        // Вторая задача: Сортировка по значениям
        Job sortByValueJob = Job.getInstance(conf, "sort by value");
        sortByValueJob.setJarByClass(SalesAnalysisJob.class);
        sortByValueJob.setMapperClass(ValueAsKeyMapper.class);
        sortByValueJob.setReducerClass(ValueAsKeyReducer.class);
        sortByValueJob.setMapOutputKeyClass(DoubleWritable.class);
        sortByValueJob.setMapOutputValueClass(Text.class);
        sortByValueJob.setOutputKeyClass(Text.class);
        sortByValueJob.setOutputValueClass(DoubleWritable.class);
        sortByValueJob.setSortComparatorClass(DescendingIntComparator.class);

        FileInputFormat.addInputPath(sortByValueJob, intermediateOutput);
        FileOutputFormat.setOutputPath(sortByValueJob, new Path(args[2]));

        System.exit(sortByValueJob.waitForCompletion(true) ? 0 : 1);
    }
}


