package org.jiazhao.combineTextinputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//KEYIN,map阶段输入的key类型
// VALUEIN,
// KEYOUT,
// VALUEOUT
public class wordcountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

     private Text outk = new Text();
   private IntWritable outV =   new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split(" ");

        for(String word:words) {
            outk.set(word);
        }
        context.write(outk,outV);

    }
}
