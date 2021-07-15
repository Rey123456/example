package com.example.configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ClassName ConfigurationExample
 * @Description TODO
 * @Author rey
 * @Date 2019/3/29 下午1:33
 */
public class ConfigurationExample extends Configured implements Tool {
    public static void main(String[] args) throws Exception{

        /**
         * 均输出blue
         * 通过set设置的属性优先级比通过addResource设置要高
         * */
        Configuration conf1 = new Configuration(false);
        conf1.addResource(new Path("iteblog.xml"));
        conf1.set("color", "blue");
        System.out.println(conf1.get("color"));

        Configuration conf2 = new Configuration(false);
        conf2.set("color", "blue");
        conf2.addResource(new Path("iteblog.xml"));
        System.out.println(conf2.get("color"));

        /**
         * 通过相同方式（都是通过 set 、或都是通过 addResource）设置属性，后面的会覆盖掉前面的设置；
         * 但是如果都是通过 addResource 设置，而且前面有些属性使用了final属性，那么后面的设置不能覆盖前面的设置；
         * */
        Configuration conf3 = new Configuration(false);
        conf3.addResource(new Path("mapreduce/src/main/resources/iteblog.xml"));
        conf3.addResource(new Path("mapreduce/src/main/resources/iteblog1.xml"));
        System.out.println(conf3.get("color"));//yellow
        Configuration conf4 = new Configuration(false);
        conf4.addResource(new Path("mapreduce/src/main/resources/iteblog1.xml"));
        conf4.addResource(new Path("mapreduce/src/main/resources/iteblog.xml"));
        System.out.println(conf4.get("color"));//red
        Configuration conf5 = new Configuration(false);
        conf5.set("color", "yellow");
        conf5.set("color", "red");
        System.out.println(conf5.get("color"));
        Configuration conf6 = new Configuration(false);
        conf6.addResource(new Path("mapreduce/src/main/resources/iteblog2.xml"));
        conf6.addResource(new Path("mapreduce/src/main/resources/iteblog1.xml"));
        System.out.println(conf6.get("color"));//yellow

        System.exit(ToolRunner.run(new ConfigurationExample(), args));
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        String input = conf.get("myjob.input");
        String output = conf.get("myjob.output");


        //必须在新建job，传conf前配置好conf相关信息，后续设置的将不起作用
        conf.set("myjob.config", "/user/tmp");

        Job job = Job.getInstance(conf, "result_getmerge");

        //不起作用，在mapper，reducer中访问不到
        conf.set("myjob.config1", "/user/tmp1");

        job.setJarByClass(ConfigurationExample.class);
        job.setMapperClass(ConfigurationExampleMapper.class);
        job.setReducerClass(ConfigurationExampleReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(input));

        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(output);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        job.setNumReduceTasks(16);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ConfigurationExampleMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            System.out.println(context.getConfiguration().get("myjob.config"));
            System.out.println(context.getConfiguration().get("myjob.config1"));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class ConfigurationExampleReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }
}