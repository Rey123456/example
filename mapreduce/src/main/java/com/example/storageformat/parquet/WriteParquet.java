package com.example.storageformat.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

import java.io.IOException;

/**
 * @Author:
 * @Description:
 * @Date: Created in 11:02 AM 8/23/18
 * @Modified by:
 */
public class WriteParquet extends Configured implements Tool {

    public static final String IN_PATH = "com.hadooptest.input";
    public static final String OUT_PATH = "com.hadooptest.output";

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("parquet.example.schema", new Schema().getSchema());
        System.out.println(conf.get("parquet.example.schema"));

        Job job = Job.getInstance(conf, "writeParquet example");
        job.setJarByClass(WriteParquet.class);
        job.setMapperClass(ParquetMapper.class);

        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Void.class);
        job.setMapOutputValueClass(Group.class);
        //设置输入
        String in = conf.get(IN_PATH);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(in));

        //设置输出
        String out = conf.get(OUT_PATH);
        job.setOutputFormatClass(ParquetOutputFormat.class);
        ParquetOutputFormat.setOutputPath(job, new Path(out));
        ParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);

        job.submit();
        return job.waitForCompletion(true)? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WriteParquet(), args));
    }

    public static class ParquetMapper extends Mapper<LongWritable, Text, Void, Group> {
        private SimpleGroupFactory factory;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(context.getConfiguration()));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 输入内容格式 name,sex,age,phone1,phone2
            String[] input = value.toString().split(",");
            System.out.println(value.toString());
            for(int i=0;i<input.length ;i++) {
                System.out.println(input[i]);
            }
            Group group = factory.newGroup().append(Fields.NAME, input[0].trim());
            if(input.length > 1 && !input[1].equals("")){
                group.append(Fields.SEX, input[1].trim());
            }
            if(input.length > 2 && !input[2].equals("")){
                group.append(Fields.AGE, Integer.parseInt(input[2].trim()));
            }
            if(input.length > 3 && !input[3].equals("")){
                for(int i=3; i<input.length; i++){
                    if(!input[i].equals("")){
                        group.append(Fields.PHONE, input[i].trim());
                    }
                }
            }
            context.write(null, group);
        }
    }
}

