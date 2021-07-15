package com.example;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.junit.After;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @ClassName BaseTest
 * @Description TODO
 * @Author rey
 * @Date 2019/3/26 下午6:24
 */
public class BaseTest extends HadoopTestCase {

    public BaseTest() throws IOException {
        super(LOCAL_MR, LOCAL_FS, 1, 1);
    }

    @After
    public void tearDown(String[] outputs) throws Exception {
        for(String tmp : outputs){
            getFileSystem().delete(new Path(tmp), true);
        }
        super.tearDown();
    }

    @After
    public void tearDown(Path[] outputs) throws Exception {
        for(Path tmp : outputs){
            getFileSystem().delete(tmp, true);
        }
        super.tearDown();
    }

    protected static final Path BASEDIR = new Path("src/test/resources");

    public void runValidator(Path output, Path exceptedOutput) throws Exception {
        //将两个结果的数据做对比
        Set<String> exceptedOut = new HashSet<String>();
        FileSystem fs = getFileSystem();
        FileStatus[] contents = fs.globStatus(exceptedOutput);
        Arrays.sort(contents);
        for(int i = 0; i < contents.length; ++i) {
            if (contents[i].isFile()) {
                FSDataInputStream in = fs.open(contents[i].getPath());
                BufferedReader reader = null;
                String line;
                try {
                    reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
                    while ((line = reader.readLine()) != null) {
                        exceptedOut.add(line);
                    }
                } finally {
                    in.close();
                    reader.close();
                }
            }
        }
        FileStatus[] contentsout =  fs.globStatus(output);
        int size = 0;
        for(FileStatus status : contentsout){
            if(status.isFile()){
                FSDataInputStream in = fs.open(status.getPath());
                BufferedReader reader = null;
                String line;
                try {
                    reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
                    while ((line = reader.readLine()) != null) {
                        if(!exceptedOut.contains(line)){
                            throw new Exception(output.toString() + " have different data with " + exceptedOutput.toString() + ":\n" + line);
                        }
                        size++;
                    }
                } finally {
                    in.close();
                    reader.close();
                }
            }
        }
        if(size != exceptedOut.size()){
            throw new Exception(output.toString() + " have size is "+ size + ",\n but "+ exceptedOutput.toString() + " is " + exceptedOut.size());
        }
    }
}