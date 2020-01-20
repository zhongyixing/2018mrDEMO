package com.zhongyx.bigdata.contest.demo2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//按年度统计各个部门资金总发放量（如2018-民政发放量，2017-民政发放量、教育等）（5分）
public class Demo2Reducer extends Reducer<Text, DoubleWritable,Text, DoubleWritable>{

	
	DoubleWritable outputValue= new DoubleWritable();
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> value,
			Context context) throws IOException, InterruptedException {
		
		
		double sumMoney=0.0D;
		Iterator<DoubleWritable> it=value.iterator();
		while(it.hasNext())
		{
			sumMoney+=it.next().get();
		}
		
		outputValue.set(sumMoney);
		
		context.write(key, outputValue);
		
	}
}
