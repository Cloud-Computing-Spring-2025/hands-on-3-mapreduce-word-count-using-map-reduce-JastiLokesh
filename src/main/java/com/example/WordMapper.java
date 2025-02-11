package com.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Convert line to string and split into words
        String line = value.toString();
        String[] words = line.trim().split("\\s+");

        // Emit each word with count of 1
        for (String str : words) {
            if (str.length() > 0) {
                word.set(str);
                context.write(word, one);
            }
        }
    }
}
