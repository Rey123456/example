package com.example.calculateexample.wordcount;

import com.example.calculateexample.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @ClassName WordCountTest
 * @Description 借助HadoopTestCase测试
 * @Author rey
 * @Date 2019/3/26 下午6:03
 */
public class WordCountTest extends HadoopTestCase {

    private static final Path INPUT = new Path("src/test/resources/wordcountinput");
    private static final Path OUTPUT = new Path("target/output");

    public WordCountTest() throws IOException {
        super(LOCAL_MR, LOCAL_FS, 1, 1);
    }

    @After
    public void tearDown() throws Exception {
        getFileSystem().delete(OUTPUT, true);
        super.tearDown();
    }

    private void runTestWordCount() throws Exception {
        getFileSystem().delete(OUTPUT, true);
        String[] args = {INPUT.toString(), OUTPUT.toString()};
        assertEquals(ToolRunner.run(new Configuration(), new WordCount(), args), 0);
    }

    private void validateOuput() throws IOException {
        InputStream in = null;
        try {
            in = getFileSystem().open(new Path("target/output/part-r-00000"));
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

    @Test
    public void testWordCount() throws Exception {
        runTestWordCount();
        validateOuput();
    }
}