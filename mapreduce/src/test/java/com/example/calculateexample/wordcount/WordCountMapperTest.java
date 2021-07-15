package com.example.calculateexample.wordcount;

import com.example.calculateexample.WordCount;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.inOrder;


/**
 * @ClassName WordCountMapperTest
 * @Description 主要使用mockito单独测试Mapper
 * @Author rey
 * @Date 2019/3/26 下午
 */
public class WordCountMapperTest {
    private WordCount.WordCountMapper mapper;
    private Mapper.Context context;
    private IntWritable one;

    @Before
    public void init() throws IOException, InterruptedException {
        mapper = new WordCount.WordCountMapper();
        context = mock(Mapper.Context.class);
        mapper.word = mock(Text.class);
        one = new IntWritable(1);
    }
    @Test
    public void testSingleWord() throws IOException, InterruptedException {
        mapper.map(new LongWritable(1L), new Text("foo"), context);
        InOrder inOrder = inOrder(mapper.word, context);
        assertCountedOnce(inOrder, "foo");
    }
    @Test
    public void testMultipleWords() throws IOException, InterruptedException {
        mapper.map(new LongWritable(1L), new Text("one two three four"), context);
        InOrder inOrder = inOrder(mapper.word, context, mapper.word, context, mapper.word, context, mapper.word, context);
        assertCountedOnce(inOrder, "one");
        assertCountedOnce(inOrder, "two");
        assertCountedOnce(inOrder, "three");
        assertCountedOnce(inOrder, "four");
    }
    private void assertCountedOnce(InOrder inOrder, String w) throws IOException, InterruptedException {
        inOrder.verify(mapper.word).set(eq(w));
        inOrder.verify(context).write(eq(mapper.word), eq(one));
    }
}









