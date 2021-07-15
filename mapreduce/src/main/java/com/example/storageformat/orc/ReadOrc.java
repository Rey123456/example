package com.example.storageformat.orc;

import com.example.storageformat.parquet.Fields;
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
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;

import java.io.IOException;

/**
 * @ClassName ReadOrc
 * @Description TODO
 * @Author rey
 * @Date 2019/8/16 下午3:24
 */
public class ReadOrc extends Configured implements Tool {

    public static final String IN_PATH = "com.orc.input";
    public static final String OUT_PATH = "com.orc.output";

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("orc.mapred.input.schema", new Schema().getSchema());

        Job job = Job.getInstance(conf, "readorc example");
        job.setJarByClass(ReadOrc.class);
        job.setMapperClass(ReadMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置输入
        String in = conf.get(IN_PATH);
        job.setInputFormatClass(OrcInputFormat.class);
        OrcInputFormat.addInputPath(job, new Path(in));

        //设置输出
        String out = conf.get(OUT_PATH);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out));

        job.submit();
        return job.waitForCompletion(true)? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ReadOrc(), args));
    }

    public static class ReadMapper extends Mapper<Void, OrcStruct, NullWritable, Text> {
        @Override
        protected void map(Void key, OrcStruct value, Context context) throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            String name = value.getFieldValue(Fields.NAME).toString();
            builder.append(name);
            builder.append(",");
            String sex = "";
            try {
                sex = value.getFieldValue(Fields.SEX).toString();
            }catch (RuntimeException e){
                sex = "";
            }
            builder.append(sex);
            builder.append(",");
            String age;
            try {
                age = value.getFieldValue(Fields.AGE).toString();
            }catch (RuntimeException e){
                age = "";
            }
            builder.append(age);
            builder.append(",");
            OrcList phoneNum = (OrcList)value.getFieldValue(Fields.PHONE);
            for(int i=0;i<phoneNum.size(); i++){
                builder.append(phoneNum.get(i));
                builder.append(",");
            }
            context.write(NullWritable.get(), new Text(builder.toString()));
        }
    }
}