package com.zhongyx.bigdata.contest.demo5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

//（5）分析违规发放情况，发现违法违纪重灾区，如：易出现问题的资金、人群、年度、性别等为领导决策提供参考。(10)
public class Demo5Reducer extends Reducer<Demo5Bean, DoubleWritable,Demo5Bean, DoubleWritable>{

	
	DoubleWritable outputValue= new DoubleWritable();
	
	@Override
	protected void reduce(Demo5Bean key, Iterable<DoubleWritable> values,
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
