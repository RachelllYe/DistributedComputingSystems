package cn.edu.ecnu.mapreduce.example.java.kmeans;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class KMeans extends Configured implements Tool {
    // 最大迭代次数
    public static final int MAX_ITERATION = 15;
    // 判定收敛的差值阈值
    public static final double DELTA = 1e-6;
    public static final long CENTER = 100;
    //从0开始计算当前迭代步数
    private static int iteration = 0;

    //配置项中用于记录当前迭代步数的键
    public static final String ITERATION = "1";
    // 用于计数收敛节点数的 Counter 参数
    public static final String GROUP_NAME = "KMeansCounter";
    public static final String COUNTER_NAME = "Center_Counter";

    @Override
    public int run(String[] args) throws Exception {
        /* 步骤1：设置作业的信息 */
        getConf().setInt(KMeans.ITERATION,iteration);
        Job job = Job.getInstance(getConf(), getClass().getSimpleName());
        // 设置程序的类名
        job.setJarByClass(getClass());

        // 设置数据的输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + iteration));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置map方法和输出键值对数据类型
        job.setMapperClass(KmeansMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //将聚类中心集通过分布式缓存广播出去
        if(iteration == 0){
            //第一次迭代时的聚类中心集
            job.addCacheFile(new URI(args[2]));
        }else{
            //广播上一次迭代输出的聚类中心集
            job.addCacheFile(new URI(args[1] + (iteration-1)+"/part-r-00000"));
        }

        //最后一次迭代输出的聚类结果;
        if((iteration + 1) != MAX_ITERATION){
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
        }else{
            job.setNumReduceTasks(0);
        }

        if (!job.waitForCompletion(true)
                || job.getCounters()
                .findCounter(GROUP_NAME, COUNTER_NAME)
                .getValue() == CENTER) {
            return 0;
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        /* 步骤2：运行作业 */
        int exitCode = 0;
        while(iteration < MAX_ITERATION){
            exitCode = ToolRunner.run(new KMeans(), args);
            if (exitCode == -1){
                break;
            }
            iteration++;
        }
        System.exit(exitCode);
    }
}
