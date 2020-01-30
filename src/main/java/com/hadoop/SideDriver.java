package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @ClassName: SideDriver
 * @Description:
 * @Author: xiedong
 * @Date: 2020/1/29 18:32
 */
public class SideDriver {
    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SideDriver.class);
        job.setMapperClass(SideMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Item.class);

//        job.addCacheFile(new Path("hdfs://192.168.154.10:9000/pros.txt").toUri());
        job.addCacheFile(new URI("hdfs://192.168.154.10:9000/pros.txt"));


        for (String arg : args) {
            System.out.println(arg);
        }

        FileInputFormat.addInputPath(job,new Path("hdfs://192.168.154.10:9000/order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.154.10:9000/res.txt"));

        int success = job.waitForCompletion(true) ? 0 : 1;
        System.exit(success);
    }
}
