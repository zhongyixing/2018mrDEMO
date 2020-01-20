package com.zhongyx.bigdata.contest.demo1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//按年度统计每个年度的资金发放量（5分）
public class Demo1Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	Text outputKey = new Text();
	DoubleWritable outputValue= new DoubleWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line=value.toString();
		
		String[] columns=line.split("\t");
		
		String year=columns[4].substring(0,4);
		
		Double sendMoney=Double.valueOf(columns[5]);
		
		outputKey.set(year);
		outputValue.set(sendMoney);
		
		context.write(outputKey, outputValue);
	}



	
	
}
