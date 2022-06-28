package cn.edu.ecnu.mapreduce.example.java.joinoperator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.conf.Configuration;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


/* 步骤1：确定输入键值对[K1,V1]的数据类型为[LongWritable,Text]，输出键值对 [K2,V2]的数据类型为[Text,NullWritable] */
public class JoinOperatorMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Map<String,String> table2 = new HashMap<String, String>();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if(table2.isEmpty()){
            URI uri = context.getCacheFiles()[0];
            FileSystem fs = FileSystem.get(uri, new Configuration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
            String content;
            while((content =reader.readLine()) != null){
                String[] datas = content.split(",");
                table2.put(datas[0],datas[1]);
            }
        }
        String[] datas = value.toString().split(",");

        for(String data: datas){
            if(table2.containsKey(data)){
                context.write(new Text(value.toString() + ","+table2.get(data)),NullWritable.get());
                break;
            }
        }
    }
}