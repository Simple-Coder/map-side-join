package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: DemoMapper
 * @Description:
 * @Author: xiedong
 * @Date: 2020/1/29 18:58
 */
public class SideMapper extends Mapper<LongWritable, Text,Text,Item> {
    private Map<String,Item> productMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        productMap=new HashMap<String,Item>();
        Configuration conf = context.getConfiguration();
        URI[] cacheFiles = context.getCacheFiles();
        for (URI cacheFile : cacheFiles) {
            System.out.println(cacheFile);
        }
        FileSystem fs = FileSystem.get(cacheFiles[0], conf);
        FSDataInputStream in = fs.open(new Path(cacheFiles[0]));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String line = null;

        while((line=br.readLine())!=null){
            String[] itemInfo = line.split(" ");
            Item item = new Item();
            item.setPid(itemInfo[0]);
            item.setName(itemInfo[1]);
            item.setPrice(Double.parseDouble(itemInfo[2]));
            productMap.put(item.getPid(),item);
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] orderInfo = line.split(" ");
        Item item = new Item();
        item.setId(orderInfo[0]);
        item.setDate(orderInfo[1]);
        item.setPid(orderInfo[2]);
        item.setAmount(Integer.parseInt(orderInfo[3]));

        item.setName(productMap.get(item.getPid()).getName());
        item.setPrice(productMap.get(item.getPid()).getPrice());

        context.write(new Text(item.getDate()),item);
    }

}
