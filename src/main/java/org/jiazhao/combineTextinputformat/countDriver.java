package org.jiazhao.combineTextinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class countDriver {
    public static void main(String[] args) throws Exception{
//1：这个啊
        Configuration conf = new Configuration();
         Job job1 = new Job();

        Job job = job1.getInstance(conf,"job1");
        //设置jar包路径
        job.setJarByClass(countDriver.class);
        //关联mapper和reducer
     job.setMapperClass(wordcountMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
  job.setReducerClass(wordcountReduce.class);
        //设置maop输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);



        //设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        如果不设置inputfoemat 他默认使用的Textinputformat.class
        job.setInputFormatClass(CombineFileInputFormat.class);
        CombineFileInputFormat.setMaxInputSplitSize(job,4194304);
        //设置输出和输入类型
        FileInputFormat.setInputPaths(job,new Path("H:\\hadoopstudent\\data\\inputcombinetextinputformat"));
        FileOutputFormat.setOutputPath(job,new Path("H:\\hadoopstudent\\data\\output2"));
        //提交job
        job.waitForCompletion(true);
        System.exit(true ? 0 : 1);
    }
}