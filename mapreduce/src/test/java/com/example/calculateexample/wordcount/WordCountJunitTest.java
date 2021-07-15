package com.example.calculateexample.wordcount;

import com.example.calculateexample.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.junit.Assert.assertEquals;

/**
 * @ClassName WordCountJunitTest
 * @Description 使用junit测试mr，其他测试类相同，包括：WordCountJunitTest1，
 * WordCountMapperTest，WordCountReducerTest，WordCountMapperWithCounterTest
 * @Author rey
 * @Date 2019/3/26
 */
public class WordCountJunitTest {
    private Configuration conf;
    private Path input;
    private Path output;
    private FileSystem fs;
    @Before
    public void setup() throws IOException {
        conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        input = new Path("src/test/resources/wordcountinput");
        output = new Path("target/output");
        fs = FileSystem.getLocal(conf);
        fs.delete(output, true);
    }
    @Test
    public void test() throws Exception {
        //也可以将wordcount中run里的东西完全粘贴进来进行测试，如WordCountJunitTest类，本质上是一样的
        WordCount wordCount = new WordCount();
        wordCount.setConf(conf);
        int exitCode = wordCount.run(new String[] {input.toString(), output.toString()});
        assertEquals(0, exitCode);
        validateOuput(fs);
    }

    public static void validateOuput(FileSystem fs) throws IOException {
        InputStream in = null;
        try {
            in = fs.open(new Path("target/output/part-r-00000"));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            assertEquals("five\t1", br.readLine());
            assertEquals("four\t1", br.readLine());
            assertEquals("one\t3", br.readLine());
            assertEquals("six\t1", br.readLine());
            assertEquals("three\t1", br.readLine());
            assertEquals("two\t2", br.readLine());
        } finally {
            IOUtils.closeStream(in);
        }
    }
}

