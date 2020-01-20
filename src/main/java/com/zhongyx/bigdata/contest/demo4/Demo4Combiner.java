package com.zhongyx.bigdata.contest.demo4;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//（4）按每个人资金违规领取总量分布统计人数，具体统计项：违规领取低于五千人数、五千至三万人数、三至五万、五至十万、十至二十万、二十万以上（10分）
public class Demo4Combiner extends Reducer<Text, DoubleWritable,Text, DoubleWritable>{

	
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
