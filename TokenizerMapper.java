package tn.isima.exercice;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public  class TokenizerMapper
extends Mapper<LongWritable, Text, Text, IntWritable>{
@Override
public void map(LongWritable key, Text value, Context context
             ) throws IOException, InterruptedException {
	String line = value.toString();

	String[] data=line.split(",");
	try {
		String influenceur = data[0];
		int nbre_share = Integer.parseInt(data[9]);	
		
		context.write(new Text(influenceur),new IntWritable(nbre_share));
			
	} catch(Exception e ) {
		
	}
	
		
	

}
}