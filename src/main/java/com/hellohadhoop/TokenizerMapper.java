package com.hellohadhoop;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 描述：WordCount explains by xxm
 * @author xxm
 */
public class TokenizerMapper｛

 /**
 * Map类：自己定义map方法
 */
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
    * LongWritable, IntWritable, Text 均是 Hadoop 中实现的用于封装 Java 数据类型的类
    * 都能够被串行化从而便于在分布式环境中进行数据交换，可以将它们分别视为long,int,String 的替代品。
    */
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    /**
    * Mapper类中的map方法：
    * protected void map(KEYIN key, VALUEIN value, Context context)
    * 映射一个单个的输入k/v对到一个中间的k/v对
    * Context类：收集Mapper输出的<k,v>对。
    */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }
 } 
