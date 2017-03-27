package com.cloudwick.tests;

import com.cloudwick.practise.PairMapper;
import com.cloudwick.practise.PairReducer;
import com.cloudwick.practise.TextPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Rajiv on 3/27/17.
 */
public class MRUnitTextPair {


    MapDriver<LongWritable,Text,TextPair,IntWritable> mapDriver;
    ReduceDriver<TextPair,IntWritable,TextPair,IntWritable> reduceDriver;
    MapReduceDriver<LongWritable,Text,TextPair,IntWritable, TextPair,IntWritable> mapReduceDriver;


    @Before
    public void setUp() {

        PairMapper mapper = new PairMapper();
        PairReducer reducer = new PairReducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("KansasCity:MO"));
        mapDriver.withOutput(new TextPair("KansasCity","MO"), new IntWritable(1));
        mapDriver.runTest();
    }


    @Test
    public void testReducer() throws IOException {

        List<IntWritable> myValuesMO = new ArrayList<IntWritable>();

        myValuesMO.add(new IntWritable(1));
        myValuesMO.add(new IntWritable(1));

        List<IntWritable> myValuesCA = new ArrayList<IntWritable>();

        myValuesCA.add(new IntWritable(1));

        reduceDriver.withInput(new TextPair("KansasCity","MO"), myValuesMO);
        reduceDriver.withInput(new TextPair("San Francisco","CA"), myValuesCA);
        reduceDriver.withOutput(new TextPair("KansasCity","MO"), new IntWritable(2));
        reduceDriver.withOutput(new TextPair("San Francisco","CA"), new IntWritable(1));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {

        mapReduceDriver.withInput(new LongWritable(), new Text("Newark:NJ"));
        mapReduceDriver.withInput(new LongWritable(), new Text("KansasCity:MO"));
        mapReduceDriver.withInput(new LongWritable(), new Text("San Francisco:CA"));
        mapReduceDriver.withInput(new LongWritable(), new Text("Newark:NJ"));
        mapReduceDriver.withInput(new LongWritable(), new Text("San Francisco:CA"));


        // Output is ordered based on State so if we change the order of the below statements will result in test
        // case failure
        mapReduceDriver.withOutput(new TextPair("San Francisco","CA"), new IntWritable(2));
        mapReduceDriver.withOutput(new TextPair("KansasCity","MO"), new IntWritable(1));
        mapReduceDriver.withOutput(new TextPair("Newark","NJ"), new IntWritable(2));
        mapReduceDriver.runTest();

    }







}
