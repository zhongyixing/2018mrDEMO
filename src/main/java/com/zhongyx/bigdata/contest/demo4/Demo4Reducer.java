package com.zhongyx.bigdata.contest.demo4;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//（4）按每个人资金违规领取总量分布统计人数，具体统计项：违规领取低于五千人数、五千至三万人数、三至五万、五至十万、十至二十万、二十万以上（10分）
public class Demo4Reducer extends Reducer<Text, DoubleWritable,Text, IntWritable>{

	
	int lower_5K=0;
	int between_5K_3W=0;
	int between_3W_5W=0;
	int between_5W_10W=0;
	int between_10W_20W=0;
	int above_20W=0;
	
	Text outputKey= new Text();
	IntWritable outputValue= new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		
		double sumMoney=0.0D;
		
		for(DoubleWritable money:values)
		{
			sumMoney+=money.get();
		}
		
		if(sumMoney<5000.00D)
		{
			lower_5K++;
		}
		else if(sumMoney>=5000.00D && sumMoney<30000.00D)
		{
			between_5K_3W++;
		}
		else if(sumMoney>=30000.00D && sumMoney<50000.00D)
		{
			between_3W_5W++;
		}
		else if(sumMoney>=50000.00D && sumMoney<100000.00D)
		{
			between_5W_10W++;
		}
		else if(sumMoney>=100000.00D && sumMoney<200000.00D)
		{
			between_10W_20W++;
		}
		else if(sumMoney>=200000.00D)
		{
			above_20W++;
		}
	}

	@Override
	protected void cleanup(Reducer<Text, DoubleWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		outputKey.set("低于五千人数");
		outputValue.set(lower_5K);
		context.write(outputKey,outputValue);
		
		outputKey.set("五千至三万人数");
		outputValue.set(between_5K_3W);
		context.write(outputKey,outputValue);
		
		outputKey.set("三至五万人数");
		outputValue.set(between_3W_5W);
		context.write(outputKey,outputValue);
		
		outputKey.set("五至十万人数");
		outputValue.set(between_5W_10W);
		context.write(outputKey,outputValue);
		
		outputKey.set("十至二十万人数");
		outputValue.set(between_10W_20W);
		context.write(outputKey,outputValue);
		
		outputKey.set("二十万以上人数");
		outputValue.set(above_20W);
		context.write(outputKey,outputValue);
		
	}
}
