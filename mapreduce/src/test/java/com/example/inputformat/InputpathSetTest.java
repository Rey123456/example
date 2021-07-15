package com.example.inputformat;

import com.example.BaseTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.IOException;

/**
 * @ClassName InputpathSetTest
 * @Description TODO
 * @Author rey
 * @Date 2019/3/26 下午6:23
 *
 * public static String FILE_ADD_INPUT = "com.input.addinput";
 *     public static String FILE_ADD_INPUT1 = "com.input.addinput1";
 *     public static String FILE_SET_INPUT = "com.input.setinput";
 *     public static String FILE_GLOBPATH = "com.input.globpath";
 *     public static String FILE_OUTPUT = "com.output.outpath";
 */
public class InputpathSetTest extends BaseTest {

    public InputpathSetTest() throws IOException {

    }

    @Test
    public void testInputpathSet() throws Exception {
        //输出删除
        getFileSystem().delete(new Path(BaseTest.BASEDIR.toString().concat("/output")), true);

        //启动测试
        String[] args = {};
        Configuration conf = new Configuration();
        conf.set(InputpathSet.FILE_ADD_INPUT, BaseTest.BASEDIR.toString().concat("/inputpath"));
        conf.set(InputpathSet.FILE_ADD_INPUT1, BaseTest.BASEDIR.toString().concat("/inputpath1"));
        conf.set(InputpathSet.FILE_SET_INPUT, BaseTest.BASEDIR.toString().concat("/inputpath"));
        conf.set(InputpathSet.FILE_GLOBPATH, BaseTest.BASEDIR.toString().concat("/*"));
        conf.set(InputpathSet.FILE_OUTPUT, BaseTest.BASEDIR.toString().concat("/output"));
        assertEquals(ToolRunner.run(conf, new InputpathSet(), args), 0);

        //结果删除
        getFileSystem().delete(new Path(BaseTest.BASEDIR.toString().concat("/output")), true);
    }
}