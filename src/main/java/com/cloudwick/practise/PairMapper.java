package com.cloudwick.practise;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Rajiv on 3/21/17.
 */
public class PairMapper extends Mapper<LongWritable,Text,TextPair,IntWritable> {

    private final IntWritable ONE = new IntWritable(1);
    private TextPair obj;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String input[] = value.toString().split(":");
        obj = new TextPair(input[0],input[1]);
        context.write(obj,ONE);
    }

}
