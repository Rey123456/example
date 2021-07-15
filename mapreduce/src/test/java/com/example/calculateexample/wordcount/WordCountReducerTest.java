package com.example.calculateexample.wordcount;

import com.example.calculateexample.WordCount;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @ClassName WordCountReducerTest
 * @Description 主要使用mockito单独测试reducer
 * @Author rey
 * @Date 2019/3/26
 */
public class WordCountReducerTest {
    private WordCount.WordCountReducer reducer;
    private Reducer.Context context;
    @Before
    public void init() throws IOException, InterruptedException {
        reducer = new WordCount.WordCountReducer();
        context = mock(Reducer.Context.class);
    }
    @Test
    public void testSingleWord() throws IOException, InterruptedException {
        List<IntWritable> values = Arrays.asList(new IntWritable(1), new IntWritable(4), new IntWritable(7));
        reducer.reduce(new Text("foo"), values, context);
        verify(context).write(new Text("foo"), new IntWritable(12));
    }
}
