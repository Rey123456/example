package com.example.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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

import java.io.IOException;

/**
 * @ClassName InputpathSet
 * @Description 测试关于输入路径的问题，模式匹配的情况
 * @Author rey
 * @Date 2019/3/26 下午3:43
 */
public class InputpathSet extends Configured implements Tool {

    public static String FILE_ADD_INPUT = "com.input.addinput";
    public static String FILE_ADD_INPUT1 = "com.input.addinput1";
    public static String FILE_SET_INPUT = "com.input.setinput";
    public static String FILE_GLOBPATH = "com.input.globpath";
    public static String FILE_OUTPUT = "com.output.outpath";

    public class ExcludePathFilter implements PathFilter {
        private final String regex;
        public ExcludePathFilter(String regex){
            this.regex = regex;
            System.out.println(regex);
        }

        public boolean accept(Path path) {
            System.out.println(path.toString() + ":" + path.toString().matches(regex));
            return !path.toString().matches(regex);
        }
    }
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "inputpathSet");

        /**getglobs
         * 利用路径格式带模式的得到需要的路径，eg：/hdfsdata/2018*.csv
         */
        FileSystem fs = FileSystem.get(conf);
        System.out.println(conf.get(FILE_GLOBPATH) + "\n" + "globStatus(Path path)");
        FileStatus[] fstatus = fs.globStatus(new Path(conf.get(FILE_GLOBPATH)));
        if(fstatus.length > 0) {
            for (FileStatus fst : fstatus) {
                System.out.println(fst.getPath());
            }
        }
        /**getglobsfilter
         * 利用模式匹配去除掉path中的部分文件，亦可保留，看传入的PathFilter实现
         */
        System.out.println("globStatus(Path path, PathFilter filter)");
        FileStatus[] fstatustemp = fs.globStatus(new Path(conf.get(FILE_GLOBPATH)), new ExcludePathFilter(".*.log.0.*"));
        if(fstatustemp!= null && fstatustemp.length > 0) {
            for (FileStatus fst : fstatustemp) {
                System.out.println(fst.getPath());
            }
        }

        /**addinput,setinput
         * addinput会连带上之前已经存在的目录，而setinput会直接替换
         */
        job.setInputFormatClass(TextInputFormat.class);
        System.out.println("start :" + conf.get("mapreduce.input.fileinputformat.inputdir"));
        TextInputFormat.addInputPath(job, new Path(conf.get(FILE_ADD_INPUT)));
        TextInputFormat.addInputPath(job, new Path(conf.get(FILE_ADD_INPUT1)));
        //setinput
        TextInputFormat.setInputPaths(job, new Path(conf.get(FILE_SET_INPUT)));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(conf.get(FILE_OUTPUT)));

        job.setJarByClass(InputpathSet.class);
        job.setMapperClass(InputpathSetMapper.class);
        job.setReducerClass(InputpathSetReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.submit();
        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new InputpathSet(), args));
    }

    public static class InputpathSetMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            System.out.println(context.getInputSplit());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split("\\^");
            for(String s: str){
                context.write(new Text(s), new IntWritable(1));
            }
        }
    }

    public static class InputpathSetReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
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