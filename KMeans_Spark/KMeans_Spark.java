package cn.edu.ecnu.spark.example.java.kmeans;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class KMeans_Spark {
    public static final double DELTA = 1e-6;
    public static double distanceSquared(List<Integer> point1, List<Double> point2){
        double sum = 0.0;
        for(int i = 0;i<point1.size();i++){
            sum += Math.pow(point1.get(i).doubleValue()-point2.get(i),2);
        }
        return sum;
    }
    private static List<Integer> addPoints(List<Integer> p1, List<Integer> p2){
        ArrayList<Integer> ret = new ArrayList<>();
        for(int i = 0;i<p1.size();i++){
            ret.add(p1.get(i) + p2.get(i));
        }
        return ret;
    }

    private static Integer closestPoint(List<Integer> point, List<List<Double>> kPoints2){
        int bestIndex = 0;
        double cloest = Double.POSITIVE_INFINITY;
        for(int i =0;i<kPoints2.size();i++){
            double dist = distanceSquared(point,kPoints2.get(i));
            if(dist<cloest){
                cloest = dist;
                bestIndex = i;
            }
        }
        return bestIndex;
    }

    private static boolean stopIteration(List<List<Double>>oldCenter, List<List<Double>>newCenter){
        boolean flag = true;
        for(int i = 0;i < oldCenter.size();i++){
            List<Double> oldC = oldCenter.get(i);
            List<Double> newC = newCenter.get(i);
            double distance = 0.0;
            for(int j = 0;j<oldC.size();j++){
                distance = distance + Math.pow(oldC.get(j)-newC.get(j),2);
            }
            distance = Math.sqrt(distance);
            if (distance> DELTA){
                flag = false;
                break;
            }
        }
        return flag;
    }

    public static void run(String[] args) {

        /* 步骤1：通过SparkConf设置配置信息，并创建SparkContext */
        SparkConf conf = new SparkConf();
        conf.setAppName("KMeans_Spark");
//        conf.setMaster("local"); // 仅用于本地进行调试，如在集群中运行则删除本行
        JavaSparkContext sc = new JavaSparkContext(conf);
        Integer iterateNum = 15;

        /* 步骤2：按应用逻辑使用操作算子编写DAG，其中包括RDD的创建、转换和行动等 */
        // 读入数据点，并持久化到内存
        JavaRDD<List<Integer>> points = sc.textFile(args[0]).map(
                new Function<String, List<Integer>>() {
                    @Override
                    public List<Integer> call(String s) throws Exception {
                        String[] data = s.split(" ");
                        ArrayList<Integer> ret = new ArrayList<>();
                        for(int i = 0;i < data.length -1 ;i++){
                            ret.add(Integer.parseInt(data[i]));
                        }
                        return ret;
                    }
                }
        ).cache();

        //读入聚类中心并收集到driver中
        JavaRDD<List<Double>> kPoints = sc.textFile(args[1]).map(
                new Function<String, List<Double>>() {
                    @Override
                    public List<Double> call(String s) throws Exception {
                        String[] data = s.split(" ");
                        ArrayList<Double> ret = new ArrayList<>();
                        for(int i =0;i<data.length;i++){
                            ret.add(Double.parseDouble(data[i]));
                        }
                        return ret;
                    }
                }
        );
        List<List<Double>> kPoints2 = kPoints.collect();

//        Broadcast<List<List<Double>>> kPoints2BroadCast= sc.broadcast(kPoints2);
        //进行迭代计算
        for(int iter = 0; iter<iterateNum;iter ++){
            final List<List<Double>> kPoints3 = new ArrayList<>(kPoints2);
            // 计算每个点最近的聚类中心---->类别，点，个数
            JavaPairRDD<Integer, Tuple2<List<Integer>,Integer>> closet = points.mapToPair(
                    new PairFunction<List<Integer>, Integer, Tuple2<List<Integer>, Integer>>() {
                        @Override
                        public Tuple2<Integer, Tuple2<List<Integer>, Integer>> call(List<Integer> integers) throws Exception {
                            return new Tuple2<>(closestPoint(integers,kPoints3),new Tuple2<>(integers,1));
                        }
                    }
            );

            // 将同一类别的点聚类，并将每个维度求和，点数相加---->类别，点每个维度求和，点数目
            JavaPairRDD<Integer, Tuple2<List<Integer>,Integer>> newPoints = closet.reduceByKey(
                    new Function2<Tuple2<List<Integer>, Integer>, Tuple2<List<Integer>, Integer>, Tuple2<List<Integer>, Integer>>() {
                        @Override
                        public Tuple2<List<Integer>, Integer> call(Tuple2<List<Integer>, Integer> listIntegerTuple2, Tuple2<List<Integer>, Integer> listIntegerTuple22) throws Exception {
                            return new Tuple2<>(addPoints(listIntegerTuple2._1,listIntegerTuple22._1),listIntegerTuple2._2 + listIntegerTuple22._2);
                        }
                    }
        );

            // 每个维度求均值，得到新的聚类中心
            JavaRDD<List<Double>> newPoints2 = newPoints.map(
                    new Function<Tuple2<Integer, Tuple2<List<Integer>, Integer>>, List<Double>>() {
                        @Override
                        public List<Double> call(Tuple2<Integer, Tuple2<List<Integer>, Integer>> integerPairTuple2) throws Exception {
                            //得到数目
                            Integer n = integerPairTuple2._2._2;
                            //得到点的和
                            List<Integer> point = integerPairTuple2._2._1;
                            ArrayList<Double> newPoint = new ArrayList<>();
                            for(int i = 0; i<point.size();i++){
                                newPoint.add(0.0);
                                newPoint.set(i,newPoint.get(i)+point.get(i).doubleValue() / n);
                            }
                            return newPoint;
                        }
                    }
            );

            List<List<Double>> newPoints3 = newPoints2.collect();
            boolean stop = stopIteration(kPoints2,newPoints3);
            //更新中心点
            kPoints2 = new ArrayList<>(newPoints3);

            if(iter == iterateNum -1 || stop == true){
                JavaPairRDD<List<Integer>,Integer> result = closet.mapToPair(
                        new PairFunction<Tuple2<Integer, Tuple2<List<Integer>, Integer>>, List<Integer>, Integer>() {
                            @Override
                            public Tuple2<List<Integer>, Integer> call(Tuple2<Integer, Tuple2<List<Integer>, Integer>> integerPairTuple2) throws Exception {
                                return new Tuple2<List<Integer>,Integer>( integerPairTuple2._2._1,integerPairTuple2._1);
                            }
                        }
                );
                // 输出词频统计结果到文件
                result.saveAsTextFile(args[2]);
                /* 步骤3：关闭SparkContext */
                sc.stop();
                break;
            }
        }


    }

    public static void main(String[] args) {
        run(args);
    }
}
