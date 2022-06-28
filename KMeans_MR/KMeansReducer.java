package cn.edu.ecnu.mapreduce.example.java.kmeans;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/* 步骤1：确定输入键值对[K2,List(V2)]的数据类型为[Text, Text]，输出键值对[K3,V3]的数据类型为[Text,NullWritable] */
public class KMeansReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<List<Double>> points = new ArrayList<List<Double>>();
        for(Text text:values){
            String value = text.toString();
            List<Double> point = new ArrayList<Double>();
            for(String s:value.split(" ")){
                point.add(Double.parseDouble(s));
            }
            points.add(point);
        }

        String[] olds = key.toString().split(" ");

        //计算新的聚类中心
        StringBuilder newCenter = new StringBuilder();
        for(int i = 0; i< points.get(0).size(); i++){
            double sum = 0;
            for(List<Double> data: points){
                sum += data.get(i);
            }
            newCenter.append(sum/points.size());
            newCenter.append(" ");
        }
        String[] news = newCenter.toString().split(" ");
        boolean stop = stopIteration(olds,news);
        if(stop){
            Counter ct = (Counter) context.getCounter(KMeans.GROUP_NAME, KMeans.COUNTER_NAME);
            ct.increment(1);

        }
        context.write(new Text(newCenter.toString()), NullWritable.get());

    }

    private static boolean stopIteration(String[] oldCenter, String[] newCenter){
        boolean flag = true;
        double distance = 0.0;
        for(int i = 0;i < oldCenter.length;i++) {
            distance = distance + Math.pow(Double.parseDouble(oldCenter[i]) - Double.parseDouble(newCenter[i]), 2);
        }
        distance = Math.sqrt(distance);
        if (distance> KMeans.DELTA){
            flag = false;
        }
        return flag;
    }

}
