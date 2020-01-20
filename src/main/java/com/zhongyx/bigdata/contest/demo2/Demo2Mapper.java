package com.zhongyx.bigdata.contest.demo2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//按年度统计各个部门资金总发放量（如2018-民政发放量，2017-民政发放量、教育等）（5分）
public class Demo2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	Text outputKey = new Text();
	DoubleWritable outputValue= new DoubleWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line=value.toString();
		
		String[] columns=line.split("\t");
		
		String year=columns[4].substring(0,4);
		
		String dept=columns[1];
		
		Double sendMoney=Double.valueOf(columns[5]);
		
		outputKey.set(year+"-"+dept);
		outputValue.set(sendMoney);
		
		context.write(outputKey, outputValue);
	}



	
	
}
