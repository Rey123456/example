package com.example.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;
import java.util.*;

/**
 * @ClassName InputpathSet
 * @Description 利用文件名和大小combine小文件
 * @Author rey
 * @Date 2019/3/26 下午3:43
 */
public class CombineTextInputFormatbyFileName extends FileInputFormat{

    public void addtoMap(Map<String, List<FileSplit>> map, String key, FileSplit fs){
        if(!map.containsKey(key)){
            map.put(key, new ArrayList<FileSplit>());
        }
        map.get(key).add(fs);
    }

    private Long maxSize = 0l;
    /**
     * 根据文件名将源文件分成若干split
     * 所以split切分的时候，另文件名相同的分在一起
     * 但是这样可能会出现数据倾斜的问题，所以加上了maxsize的限制，大于这个值的话就继续切分
     */
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        maxSize = job.getConfiguration().getLong("mapreduce.input.fileinputformat.split.maxsize", 128 * 1024 * 1024L);

        //获取这个路径下所有文件信息
        List<InputSplit> orig_splits = super.getSplits(job);
        Map<String, List<FileSplit>> map = new HashMap<String, List<FileSplit>>(); //文件名对应的文件信息
        Map<String, Long> mapSize = new HashMap<String, Long>(); //一类文件名对应的文件大小总和
        FileSplit fs;
        String key;
        //遍历文件信息，按文件名和文件大小进行规划
        for(Iterator i$ = orig_splits.iterator(); i$.hasNext(); ) {
            InputSplit s = (InputSplit)i$.next();
            fs = (FileSplit)s;
            key = fs.getPath().getName().split("\\.")[1];
            boolean putin = false;
            while(mapSize.containsKey(key)){
                if(mapSize.get(key) < maxSize) {
                    mapSize.put(key, fs.getLength() + mapSize.get(key));
                    addtoMap(map, key, fs);
                    putin = true;
                    break;
                }else{
                    key = key + "1"; //区别相同文件名但是大小达到限制的
                }
            }
            if(!putin){
                mapSize.put(key, fs.getLength());
                addtoMap(map, key, fs);
            }
        }

        List<InputSplit> splits = new ArrayList<InputSplit>();
        Iterator i$ = map.keySet().iterator();

        //对属于一组的文件进行combine
        while(i$.hasNext()) {
            String keyname = (String)i$.next();
            System.out.println(keyname);
            System.out.println(mapSize.get(keyname));
            int size = map.get(keyname).size();
            Path[] paths = new Path[size];
            long[] lengths = new long[size];
            int i = 0;

            for(Iterator j = ((List)map.get(keyname)).iterator(); j.hasNext(); ++i) {
                fs = (FileSplit)j.next();
                paths[i] = fs.getPath();
                lengths[i] = fs.getLength();
            }
            splits.add(new CombineFileSplit(paths, lengths));
        }
        System.out.println("Splits total num=" + splits.size());
        return splits;
    }

    //Reader完全是从CombineTextInputFormat类中copy过来的
    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new CombineFileRecordReader<LongWritable,Text>(
                (CombineFileSplit)split, context, TextRecordReaderWrapper.class);
    }
    private static class TextRecordReaderWrapper
            extends CombineFileRecordReaderWrapper<LongWritable,Text> {
        // this constructor signature is required by CombineFileRecordReader
        public TextRecordReaderWrapper(CombineFileSplit split,
                                       TaskAttemptContext context, Integer idx)
                throws IOException, InterruptedException {
            super(new TextInputFormat(), split, context, idx);
        }
    }
}
