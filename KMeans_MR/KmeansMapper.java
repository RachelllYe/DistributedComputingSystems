package cn.edu.ecnu.mapreduce.example.java.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/* 步骤1：确定输入键值对[K1,V1]的数据类型为[LongWritable,Text]，输出键值对 [K2,V2]的数据类型为[Text,Text] */
public class KmeansMapper extends Mapper<LongWritable, Text, Text, Text> {

    private List<List<Double>> centers = new ArrayList<List<Double>>();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] dimensions;
        List<Double> point = new ArrayList<Double>();
        Integer centerIndex = 1;
        double minDistance = Double.MAX_VALUE;

        int iteration = context.getConfiguration().getInt(KMeans.ITERATION, 0);

        if(centers.size() == 0){
            URI uri = context.getCacheFiles()[0];
            FileSystem fs = FileSystem.get(uri, new Configuration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
            String content;

            while((content =reader.readLine()) != null){
                List<Double> centerPoint = new ArrayList<Double>();
                String[] datas = content.split(" ");
                for(int i = 0;i < datas.length -1 ;i++){
                    centerPoint.add(Double.parseDouble(datas[i]));
                }
                centers.add(centerPoint);
            }
        }

        dimensions = value.toString().split(" ");
        //前两个点为数据点
        for(int i = 0;i<dimensions.length -1 ;i++){
            point.add(Double.parseDouble(dimensions[i]));
        }

//        System.out.println("mapper");
//        System.out.println(point.size());
//        System.out.println("---------------");

        List<Double> centerBelong = null;
        //遍历聚类中心集并计算与数据点的距离
        for(int i = 0; i < centers.size();i++){
            double distance = 0;
            List<Double> center = centers.get(i);
            //计算数据点与当前中心点的距离
            for(int j = 0; j < center.size();j++){
                distance += Math.pow((point.get(j) - center.get(j)), 2);
            }
            distance = Math.sqrt(distance);
            if(distance < minDistance){
                minDistance = distance;
                centerIndex = i + 1;
                centerBelong = new ArrayList<Double>(center);
            }
        }

        StringBuilder centerPointBuilder = new StringBuilder();
        for(int i = 0;i<centerBelong.size();i++){
            centerPointBuilder.append(String.valueOf(centerBelong.get(i))+" ");
        }
        String centerPoint = centerPointBuilder.toString();

        StringBuilder pointDataBuilder = new StringBuilder();
        for(int i = 0;i<dimensions.length -1;i++){
            pointDataBuilder.append(dimensions[i]+" ");
        }
        String pointData = pointDataBuilder.toString();

        if(iteration == (KMeans.MAX_ITERATION -1 )){
            //数据点和类别
            context.write(new Text(pointData), new Text(String.valueOf(centerIndex)));
        }else{
            //中心点+键值对
            context.write(new Text(centerPoint), new Text(pointData));
        }
    }
}