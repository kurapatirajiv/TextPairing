package com.cloudwick.practise;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Rajiv on 3/21/17.
 */
public class PairReducer extends Reducer<TextPair,IntWritable,Text,IntWritable> {


    public void reduce(TextPair key,Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{

        int count = 0;
        for(IntWritable val: values){
            count +=val.get();
        }
        context.write(new Text(key.toString()+","),new IntWritable(count));
    }

}
