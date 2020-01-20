package com.zhongyx.bigdata.contest.demo1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//按年度统计每个年度的资金发放量（5分）
public class Demo1Reducer extends Reducer<Text, DoubleWritable,Text, DoubleWritable>{

	
	DoubleWritable outputValue= new DoubleWritable();
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		
		
		double sumMoney=0.0D;
		
		for(DoubleWritable money:values)
		{
			
			sumMoney+=money.get();
		}
		
		outputValue.set(sumMoney);
		
		context.write(key, outputValue);
		
	}
}
