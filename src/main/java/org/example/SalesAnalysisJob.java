package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.calc.SalesData;
import org.example.calc.SalesMapper;
import org.example.calc.SalesReducer;
import org.example.sort.ValueAsKeyData;
import org.example.sort.ValueAsKeyMapper;
import org.example.sort.ValueAsKeyReducer;


public class SalesAnalysisJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: SalesPipelineJob <input path> <intermediate output path> <final output path>");
            System.exit(-1);
        }

        String inputDir = args[0];
        String intermediateResultDir = args[1];
        String outputDir = args[2];

        Configuration conf = new Configuration();

        // Анализ продаж
        Job salesAnalysisJob = Job.getInstance(conf, "sales analysis");
        salesAnalysisJob.setJarByClass(SalesAnalysisJob.class);
        salesAnalysisJob.setMapperClass(SalesMapper.class);
        salesAnalysisJob.setReducerClass(SalesReducer.class);
        salesAnalysisJob.setMapOutputKeyClass(Text.class);
        salesAnalysisJob.setMapOutputValueClass(SalesData.class);
        salesAnalysisJob.setOutputKeyClass(Text.class);
        salesAnalysisJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(salesAnalysisJob, new Path(inputDir));
        Path intermediateOutput = new Path(intermediateResultDir);
        FileOutputFormat.setOutputPath(salesAnalysisJob, intermediateOutput);

        boolean success = salesAnalysisJob.waitForCompletion(true);

        if (!success) {
            System.exit(1);
        }

        // Сортировка
        Job sortByValueJob = Job.getInstance(conf, "sorting by revenue");
        sortByValueJob.setJarByClass(SalesAnalysisJob.class);
        sortByValueJob.setMapperClass(ValueAsKeyMapper.class);
        sortByValueJob.setReducerClass(ValueAsKeyReducer.class);

        sortByValueJob.setMapOutputKeyClass(DoubleWritable.class);
        sortByValueJob.setMapOutputValueClass(ValueAsKeyData.class);

        sortByValueJob.setOutputKeyClass(ValueAsKeyData.class);
        sortByValueJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sortByValueJob, intermediateOutput);
        FileOutputFormat.setOutputPath(sortByValueJob, new Path(outputDir));

        System.exit(sortByValueJob.waitForCompletion(true) ? 0 : 1);
    }
}


