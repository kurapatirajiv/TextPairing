package com.cloudwick.practise;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * Created by Rajiv on 3/21/17.
 */
public class TextPairDriver {

    public static void main(String args[]) throws IOException,InterruptedException, ClassNotFoundException{

        if(args.length!=2){
            System.out.println("Not enough arguments");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf);

        job.setJarByClass(TextPairDriver.class);
        job.setJobName("TextPair Count Application");

        job.setMapperClass(PairMapper.class);
        // Adding a combiner
        job.setCombinerClass(PairReducer.class);
        job.setReducerClass(PairReducer.class);

        // Define Mapper output classes
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

}
