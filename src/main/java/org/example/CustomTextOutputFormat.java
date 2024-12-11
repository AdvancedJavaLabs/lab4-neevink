package org.example;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CustomTextOutputFormat extends TextOutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {

        Path file = getDefaultWorkFile(job, ".txt");
        FileSystem fs = file.getFileSystem(job.getConfiguration());
        FSDataOutputStream fileOut = fs.create(file, false);

        return new CustomRecordWriter(fileOut);
    }

    public static class CustomRecordWriter extends RecordWriter<Text, Text> {
        private DataOutputStream out;

        public CustomRecordWriter(DataOutputStream out) {
            this.out = out;
        }

        @Override
        public void write(Text key, Text value) throws IOException {
            // Ваш кастомный формат, например, разбиение табуляцией или другим символом
            out.writeBytes(key.toString() + ": " + value.toString() + "\n");
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }
}

