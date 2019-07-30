package com.union.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Driver {

    private static Log log = LogFactory.getLog(Driver.class);

    public static void main(String[] args) {
        Configuration conf = new Configuration();

        try {
            // 0.0:首先删除输出路径的已有生成文件
            FileSystem fs = FileSystem.get(new URI(args[1]), conf);
            Path outPath = new Path(args[1]);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }

            Job job = Job.getInstance(conf);
            job.setJarByClass(Driver.class);

            // 1.0:指定输入目录
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            // 1.1:指定对输入数据进行格式化处理的类（可以省略）
            job.setInputFormatClass(TextInputFormat.class);
            // 1.2:指定自定义的Mapper类
            job.setMapperClass(UnionMapper.class);
            // 1.3:指定map输出的<K,V>类型（如果<k3,v3>的类型与<k2,v2>的类型一致则可以省略）
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // 1.4:分区（可以省略）
            job.setPartitionerClass(HashPartitioner.class);
            // 1.5:设置要运行的Reducer的数量（可以省略）
            job.setNumReduceTasks(1);
            // 1.6:指定自定义的Reducer类
            job.setReducerClass(UnionReducer.class);
            // 1.7:指定reduce输出的<K,V>类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // 1.8:指定输出目录
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // 1.9:指定对输出数据进行格式化处理的类（可以省略）
            job.setOutputFormatClass(TextOutputFormat.class);
            // 2.0:提交作业
            if (job.waitForCompletion(true))
                log.info("MR程序运行成功！");
            else
                log.info("MR程序运行失败！");

        } catch (ClassNotFoundException | IOException | InterruptedException | URISyntaxException e) {
            log.error(e);
        }
    }

}
