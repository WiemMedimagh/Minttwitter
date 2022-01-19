package tn.isima.exercice;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class NumMaxReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

int min = 0;
Text minWord = new Text();
public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
    
	int sum =100000;
    for(IntWritable x : values){
    	sum+= x.get();
        }
    if (sum<min) {
    	min= sum;
    	minWord.set(key);
    }
    }
@Override
protected void cleanup(Context context) throws IOException, InterruptedException {
	System.out.print("linfluenceur ayant le plus nombre nim  de like est "+minWord );
    context.write(minWord, new IntWritable(min));
}
}
