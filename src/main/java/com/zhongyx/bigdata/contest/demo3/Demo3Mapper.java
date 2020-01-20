package com.zhongyx.bigdata.contest.demo3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//（3）按每个人合计领取资金总量分布统计人数，具体统计项：个人合计领取低于五千人数、五千至三万人数、三至五万人数、五至十万人数、十至二十万人数、二十万以上人数（10分）
public class Demo3Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	Text outputKey = new Text();
	DoubleWritable outputValue= new DoubleWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line=value.toString();
		
		String[] columns=line.split("\t");
		
		String idenNo=columns[0];
		
		Double sendMoney=Double.valueOf(columns[5]);
		
		outputKey.set(idenNo);
		outputValue.set(sendMoney);
		
		context.write(outputKey, outputValue);
	}



	
	
}
