package cn.edu.ecnu.mapreduce.example.java.joinoperator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/* 步骤1：确定输入键值对[K2,List(V2)]的数据类型为[Text, ReduceJoinWritable]，输出键值对[K3,V3]的数据类型为[Text,NullWritable] */
public class JoinOperatorReducer extends Reducer<Text, ReduceJoinWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<ReduceJoinWritable> values, Context context)
            throws IOException, InterruptedException {
        List<String> t1 = new ArrayList<String>();
        List<String> t2 = new ArrayList<String>();

        for(ReduceJoinWritable value : values){
//            System.out.println(value.getData());
            //获取标识
            String tag = value.getTag();
            if(tag.equals(ReduceJoinWritable.Table1)){
                t1.add(value.getData());
            }else if (tag.equals(ReduceJoinWritable.Table2)){
                t2.add(value.getData());
            }
        }

        //进行连接并输出连接结果
        for(String item1:t1){
            for(String item2:t2){
                String[] datas = item2.split(",");
                //问题：与书本不同，连接属性与表名不相同
                String result = item1 + ","+datas[1];
//                System.out.println(result);
                context.write(new Text(result), NullWritable.get());
            }
        }
    }
}
