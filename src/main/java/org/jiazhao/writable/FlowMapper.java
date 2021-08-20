package org.jiazhao.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
/1、需求：统计每一个手机号耗费的总上行流量、下行流量、总流量
2、输入数据格式
7 13560436666 120.196.100.99 1116 954 200
Id 手机号码 网络ip 上行流量 下行流量 网络状态码
3、期望输出数据格式
13560436666 1116 954 2070
手机号码 上行流量 下行流量 总流量
4、Map阶段
（1）读取一行数据，切分字段
（2）抽取手机号、上行流量、下行流量
（3）以手机号为key，bean对象为value输 出， 即context.write(手机号,bean);
 5、Reduce阶段
（1）累加上行流量和下行流量得到总流量。 （4）bean对象要想能够传输，必须实现序列化接口
7 13560436666 120.196.100.99 1116 954 200
 */
//KEYIN, VALUEIN, KEYOUT, VALUEOUT
public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];

        outK.set(phone);
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();
        context.write(outK, outV);




    }
}
