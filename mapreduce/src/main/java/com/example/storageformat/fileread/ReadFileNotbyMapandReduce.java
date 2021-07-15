package com.example.storageformat.fileread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;

/**
 * @ClassName ReadFileNotbyMapandReduce
 * @Description 主要是使用方法在非map reduce过程中读取文件，包括本地文件和hdfs文件
 *  * WordCount，将本地文件put到hdfs上作为输入
 * @Author rey
 * @Date 2019/3/26 下午4:15
 */
public class ReadFileNotbyMapandReduce extends Configured implements Tool {

    public static String FILE_INPUT = "com.input.file";//在main中put到hdfs上作为输入，wordcount
    public static String FILE_INPUT_HDFS = "com.input.file.hdfs"; //put到hdfs上的路径
    public static String FILE_OUTPUT_HDFS = "com.output.file.hdfs"; // 输出结果
    public static String FILE_MAINCONFIG = "com.main.config";//在main中读取文件
    public static String FILE_HDFSCONFIG = "com.hdfs.config";//在hdfs上的文件，在setup中读
    public static String FILE_DISTRIBUTED = "com.distributed";//distributed形式,似乎需要是hdfs文件，然后会成为运行节点的本地文件
    public static String FILE_DISTRIBUTED1 = "com.distributed1";

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "hadoopTest");

        /**
         * 在主函数中读本地文件
         */
        LocalFileSystem fs = FileSystem.getLocal(conf);
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(conf.get(FILE_MAINCONFIG)))));
            String tmp;
            while ((tmp = reader.readLine()) != null) {
                //按行处理
                System.out.println(tmp);
            }
            reader.close();
        }catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        /**
         * 上传文件
         */
        try {
            Path src = new Path(conf.get(FILE_INPUT));
            Path dst = new Path(conf.get(FILE_INPUT_HDFS));
            FileSystem.get(conf).copyFromLocalFile(src, dst);
        }catch (IOException e){
            e.printStackTrace();
        }

        /**
         * 添加cache #some是加个代称，这样可以在获取的时候利用some读取到相应文件
         */
        job.addCacheFile(new Path(conf.get(FILE_DISTRIBUTED)).toUri());
        job.addCacheFile(new URI(conf.get(FILE_DISTRIBUTED1) + "#some"));

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(conf.get(FILE_INPUT_HDFS)));
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(conf.get(FILE_OUTPUT_HDFS)));

        job.setJarByClass(ReadFileNotbyMapandReduce.class);
        job.setMapperClass(ReadFileMapper.class);
        job.setReducerClass(ReadFileReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.submit();
        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ReadFileNotbyMapandReduce(), args));
    }

    public static class ReadFileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            /**
             * 读取hdfs上的文件
             */
            String hdfspath = conf.get(ReadFileNotbyMapandReduce.FILE_HDFSCONFIG);
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path(hdfspath))));
                String tmp;
                while ((tmp = reader.readLine()) != null) {
                    //按行处理
                    System.out.println(tmp);
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }

            /**
             * 读cache的文件
             */
            if(context.getCacheFiles()!=null && context.getCacheFiles().length > 0){
                File somefile = new File("./some");

                BufferedReader reader = new BufferedReader(new FileReader(somefile));
                String tmp;
                while((tmp = reader.readLine()) != null){
                    System.out.println(tmp);
                }

                URI[] files = context.getCacheFiles();
                for(URI file : files){
                    System.out.println(file);
                    //Path file1path = new Path(file);
                    //...
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split(",");
            for(String s: str){
                context.write(new Text(s), new IntWritable(1));
            }
        }
    }

    public static class ReadFileReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            for(IntWritable i : values){
                num += i.get();
            }
            context.write(NullWritable.get(), new Text(key.toString() + "," + num));
        }
    }

}