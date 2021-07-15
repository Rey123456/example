package com.example.calculateexample.wordcount;


import com.example.calculateexample.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import static org.junit.Assert.assertTrue;

/**
 * @ClassName WordCountJunitTest1
 * @Description 使用junit测试
 * @Author rey
 * @Date 2019/3/26 下午
 */
public class WordCountJunitTest1 {
    private Configuration conf;
    private Path input;
    private Path output;
    private FileSystem fs;

    @Before
    public void setup() throws IOException {
        conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        //mapred.job.tracker is deprecated. Instead, use mapreduce.jobtracker.address
        conf.set("mapreduce.jobtracker.address", "local");
        input = new Path("src/test/resources/wordcountinput");
        output = new Path("target/output");
        fs = FileSystem.getLocal(conf);
        fs.delete(output, true);

        for (FileStatus status: fs.listStatus(input)) {
            System.out.println(status.getPath());
        }
    }
    @Test
    public void testWordCount() throws Exception {

        Job job = new Job(conf);
        job.setJarByClass(WordCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCount.WordCountMapper.class);
        job.setCombinerClass(WordCount.WordCountReducer.class);
        job.setReducerClass(WordCount.WordCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        assertTrue( job.waitForCompletion(true) );
        WordCountJunitTest.validateOuput(fs);
    }
}