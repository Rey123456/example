package com.example.calculateexample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @ClassName
 * @Description
 * @Author rey
 * @Date 2019/3/26
 */
public class WordCountMapperWithCounter extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    public enum Counters {        TOTAL_WORDS    }
    // protected to allow unit testing
    public Text word = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
            context.getCounter(Counters.TOTAL_WORDS).increment(1);
        }
    }
}
