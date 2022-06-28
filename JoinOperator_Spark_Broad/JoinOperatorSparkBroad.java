package cn.edu.ecnu.spark.example.java.joinoperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class JoinOperatorSparkBroad {

    public static void run(String[] args) {
        /* 步骤1：通过SparkConf设置配置信息，并创建SparkContext */
        SparkConf conf = new SparkConf();
        conf.setAppName("JoinOperatorJava_Broad");
//        conf.setMaster("local"); // 仅用于本地进行调试，如在集群中运行则删除本行
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> table2 = sc.textFile(args[1]);

        Map<String, String> pairs2 =
                table2.mapToPair(
                        new PairFunction<String, String, String>() {
                            @Override
                            public Tuple2<String, String> call(String table2) throws Exception {
                                String[] item= table2.split(",");
                                return new Tuple2<String, String>(item[0], item[1]);
                            }
                        }).collectAsMap();
        //广播
        Broadcast<Map<String,String>> table2BroadCast = sc.broadcast(pairs2);


        /* 步骤2：按应用逻辑使用操作算子编写DAG，其中包括RDD的创建、转换和行动等 */
        // 读入文本数据，创建名为lines的RDD
        JavaRDD<String> table1 = sc.textFile(args[0]);
        String[] gender = {"Eunuch","Female","Intersex","Male","Non-Binary","Transgender","unknown"};
        ArrayList keys = new ArrayList(Arrays.asList(gender));
        JavaPairRDD<String, String> pairs1 =
                table1.mapToPair(
                        new PairFunction<String, String, String>() {
                            @Override
                            public Tuple2<String, String> call(String table1) throws Exception {
                                String[] item= table1.split(",");
                                String tp1=null;
                                for(String itm:item){
                                    if (keys.contains(itm)){
                                        tp1 = itm;
                                        break;
                                    }
                                }
                                return new Tuple2<String, String>(tp1, table1);
                            }
                        });

        JavaRDD<Tuple2<String, String>> result = pairs1.map(
                new Function<Tuple2<String, String>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                        String tb2 = null;
                        Map<String,String> table2value = table2BroadCast.getValue();
                        if (table2value.containsKey(stringStringTuple2._1)){
                            tb2=table2value.get(stringStringTuple2._1);
                        }
                        return new Tuple2<String, String>(stringStringTuple2._2,tb2);
                    }
                }
        ).filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                        return stringStringTuple2._2 != null;
                    }
                }
        );

        // 输出词频统计结果到文件
        result.saveAsTextFile(args[2]);
        /* 步骤3：关闭SparkContext */
        sc.stop();
    }

    public static void main(String[] args) {
        run(args);
    }
}
