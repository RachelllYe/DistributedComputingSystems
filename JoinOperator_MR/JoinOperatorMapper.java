package cn.edu.ecnu.mapreduce.example.java.joinoperator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/* 步骤1：确定输入键值对[K1,V1]的数据类型为[LongWritable,Text]，输出键值对 [K2,V2]的数据类型为[Text,ReduceJoinWritable] */
public class JoinOperatorMapper extends Mapper<LongWritable, Text, Text, ReduceJoinWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        //获取键值对所属文件路径
        String path = split.getPath().toString();
        ReduceJoinWritable writable = new ReduceJoinWritable();
        // 用writable保存data
        writable.setData(value.toString());
        // 注意split的符号是" "还是制表符
        String[] datas = value.toString().split(",");
//        System.out.println(datas[1].getClass());
//        System.out.println(value.toString());
        String[] gender = {"Eunuch","Female","Intersex","Male","Non-Binary","Transgender","unknown"};
        ArrayList keys = new ArrayList(Arrays.asList(gender));
        // 通过路径判断其属于哪张表
        if(path.contains("table1")){
            writable.setTag("1");
            for(String data:datas){
                if(keys.contains(data)){
                    context.write(new Text(data),writable);
                    break;
                }
            }

        }else if (path.contains("table2")){
            writable.setTag("2");
            context.write(new Text(datas[0]),writable);
        }
//        System.out.println("end");
        //假设两个表都是以第一列为key
//        System.out.println("1".getClass());
//        System.out.println(value.toString());
        //Id为key
//        context.write(new Text(datas[0]),writable);

    }
}