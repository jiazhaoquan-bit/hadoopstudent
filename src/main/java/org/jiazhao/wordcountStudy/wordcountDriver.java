package org.jiazhao.wordcountStudy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length < 2) {
//            System.err.println("Usage: wordcount <in> [<in>...] <out>");
//            System.exit(2);
//        }
//        Job job = Job.getInstance(conf, "word count");
//        job.setJarByClass(wordcount.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        for (int i = 0; i < otherArgs.length - 1; ++i) {
//            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
//        }
//        FileOutputFormat.setOutputPath(job,
//                new Path(otherArgs[otherArgs.length - 1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
public class wordcountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar包路径
        job.setJarByClass(wordcountDriver.class);
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
        //设置输出和输入类型
        FileInputFormat.setInputPaths(job,new Path("H:\\hadoopstudent\\data\\inputword"));
        FileOutputFormat.setOutputPath(job,new Path("H:\\hadoopstudent\\data\\output"));
        //提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
