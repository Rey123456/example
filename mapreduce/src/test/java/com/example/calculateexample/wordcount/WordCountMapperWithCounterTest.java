package com.example.calculateexample.wordcount;

import com.example.calculateexample.WordCountMapperWithCounter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.mockito.Mockito.inOrder;

/**
 * @ClassName WordCountMapperWithCounterTest
 * @Description
 * @Author rey
 * @Date 2019/3/26
 */
public class WordCountMapperWithCounterTest {
    private WordCountMapperWithCounter mapper;
    private Mapper.Context context;
    private Counter counter;
    private IntWritable one;
    @Before
    public void init() throws IOException, InterruptedException {
        mapper = new WordCountMapperWithCounter();
        context = mock(Mapper.Context.class);
        mapper.word = mock(Text.class);
        one = new IntWritable(1);
        counter = mock(Counter.class);
        when(context.getCounter(WordCountMapperWithCounter.Counters.TOTAL_WORDS)).thenReturn(counter);
    }
    @Test
    public void testSingleWord() throws IOException, InterruptedException {
        mapper.map(new LongWritable(1L), new Text("foo"), context);
        InOrder inOrder = inOrder(mapper.word, context, counter);
        assertCountedOnce(inOrder, "foo");
    }
    @Test
    public void testMultipleWords() throws IOException, InterruptedException {
        mapper.map(new LongWritable(1L), new Text("one two three four"), context);
        InOrder inOrder = inOrder(mapper.word, context, counter, mapper.word, context, counter, mapper.word, context, counter, mapper.word, context, counter);
        assertCountedOnce(inOrder, "one");
        assertCountedOnce(inOrder, "two");
        assertCountedOnce(inOrder, "three");
        assertCountedOnce(inOrder, "four");
    }
    private void assertCountedOnce(InOrder inOrder, String w) throws IOException, InterruptedException {
        inOrder.verify(mapper.word).set(eq(w));
        inOrder.verify(context).write(eq(mapper.word), eq(one));
        inOrder.verify(counter).increment(1);
    }
}

