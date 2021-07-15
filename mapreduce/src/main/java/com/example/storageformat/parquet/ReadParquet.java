package com.example.storageformat.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;

/**
 * @Author:
 * @Description:
 * @Date: Created in 11:02 AM 8/23/18
 * @Modified by:
 */
public class ReadParquet extends Configured implements Tool {

    public static final String IN_PATH = "com.hadooptest.input";
    public static final String OUT_PATH = "com.hadooptest.output";

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("parquet.example.schema", new Schema().getSchema());

        Job job = Job.getInstance(conf, "readParquet example");
        job.setJarByClass(ReadParquet.class);
        job.setMapperClass(ReadMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置输入
        String in = conf.get(IN_PATH);
        job.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.addInputPath(job, new Path(in));
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);

        //设置输出
        String out = conf.get(OUT_PATH);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out));

        job.submit();
        return job.waitForCompletion(true)? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ReadParquet(), args));
    }
    public static class ReadMapper extends Mapper<Void, Group, NullWritable, Text>{
        @Override
        protected void map(Void key, Group value, Context context) throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            String name = value.getString(Fields.NAME, 0);
            builder.append(name);
            builder.append(",");
            String sex = "";
            try {
                sex = value.getString(Fields.SEX, 0);
            }catch (RuntimeException e){
                sex = "";
            }
            builder.append(sex);
            builder.append(",");
            String age;
            try {
                age = String.valueOf(value.getInteger(Fields.AGE, 0));
            }catch (RuntimeException e){
                age = "";
            }
            builder.append(age);
            builder.append(",");
            int count = value.getFieldRepetitionCount(Fields.PHONE);
            for(int i=0;i<count; i++){
                builder.append(value.getString(Fields.PHONE, i));
                builder.append(",");
            }
            context.write(NullWritable.get(), new Text(builder.toString()));
        }
    }
}
