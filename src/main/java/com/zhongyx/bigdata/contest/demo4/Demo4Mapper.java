package com.zhongyx.bigdata.contest.demo4;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//（4）按每个人资金违规领取总量分布统计人数，具体统计项：违规领取低于五千人数、五千至三万人数、三至五万、五至十万、十至二十万、二十万以上（10分）
public class Demo4Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	Text outputKey = new Text();
	DoubleWritable outputValue= new DoubleWritable();
	
	//身份证号，发证时间
	Map<String, Long> breakManMap = new HashMap<String, Long>();
	
	//身份证号，关系(户主)
	Map<String, String> masterMap = new HashMap<String, String>();
	
	//身份证号，关系(非户主)
	Map<String, String> relativesMap = new HashMap<String, String>();
	
	//身份证号，死亡时间
	Map<String, Long> deadManMap = new HashMap<String, Long>();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		try
		{
			this.initBreakManMap(context);
			
			this.initRelationMap(context);
			
			this.initDeadManMap(context);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void initBreakManMap(Context context) throws IOException, ParseException
	{
		// 1 获取缓存的文件
		URI[] cacheFiles = context.getCacheFiles();
		String path = cacheFiles[0].getPath().toString();
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
		String line;
		while(StringUtils.isNotEmpty(line = reader.readLine())){
			// 2 切割
			String[] fields = line.split("\t");
			
			// 3 缓存数据到集合
			SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
			Long sendTime=sdf.parse(fields[1]).getTime();

			Long tempValue=breakManMap.get(fields[0]);
			
			if(tempValue==null)
			{
				breakManMap.put(fields[0],sendTime);
			}
			else if(tempValue!=null && sendTime>tempValue)
			{
				breakManMap.put(fields[0],sendTime);
			}
		}
		
		// 4 关流
		reader.close();
	}

	private void initRelationMap(Context context) throws IOException
	{
		// 1 获取缓存的文件
			URI[] cacheFiles = context.getCacheFiles();
			String path = cacheFiles[1].getPath().toString();
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
			String line;
			while(StringUtils.isNotEmpty(line = reader.readLine())){
				//System.out.println("relation:"+line);
				// 2 切割
				String[] fields = line.split("\t");
				
				// 3 缓存数据到集合
				if(fields.length==4)
				{
					if(fields[3].equals("户主"))
					{
						masterMap.put(fields[2],fields[3]);
					}
					else
					{
						relativesMap.put(fields[2],fields[3]);
					}
				}
				else if(fields.length==3)
				{
					relativesMap.put(fields[2],"");
				}	
			}
			
			// 4 关流
			reader.close();
	}
	
	private void initDeadManMap(Context context) throws IOException, ParseException
	{
		// 1 获取缓存的文件
		URI[] cacheFiles = context.getCacheFiles();
		String path = cacheFiles[2].getPath().toString();
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
		String line;
		while(StringUtils.isNotEmpty(line = reader.readLine())){
			
			// 2 切割
			String[] fields = line.split("\t");
			
			// 3 缓存数据到集合
			SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
			Long deadTime=sdf.parse(fields[1]).getTime();
			
			
			Long tempValue=deadManMap.get(fields[0]);
			
			if(tempValue==null)
			{
				deadManMap.put(fields[0],deadTime);
			}
			else if(tempValue!=null && deadTime<tempValue)
			{
				deadManMap.put(fields[0],deadTime);
			}
		}
		
		// 4 关流
		reader.close();
	}



	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		try
		{
			String line=value.toString();
			
			String[] columns=line.split("\t");
			
			String idenNo=columns[0];
			
			String dept=columns[1];
			
			String moneyType=columns[2];
			
			String gender=columns[3];
			
			String sendDate=columns[4];
			
			String year=columns[4].substring(0,4);
			
			Double sendMoney=Double.valueOf(columns[5]);
			
			if(deadManMap.containsKey(idenNo))
			{//如果是死亡人员
				long deadTime=deadManMap.get(idenNo);
				
				SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
				long sendTime=sdf.parse(sendDate).getTime();
				
				if((deadTime+(1000*60*60*24*180))>sendTime)
				{//死亡时间180天后，仍然有资金发放，视为异常
					outputKey.set(idenNo);
					outputValue.set(sendMoney);
					context.write(outputKey, outputValue);
					return;
				}
			}
			
			if(dept.equals("残联") && !breakManMap.containsKey(idenNo))
			{//如果是残联发放资金，但是在残疾人表找不到身份证号，视为异常
				outputKey.set(idenNo);
				outputValue.set(sendMoney);
				context.write(outputKey, outputValue);
				return;
			}
			
			if(masterMap.containsKey(idenNo))
			{//如果是户主的公职人员，视为异常
				outputKey.set(idenNo);
				outputValue.set(sendMoney);
				context.write(outputKey, outputValue);
				return;
			}
			
			if(dept.equals("民政") && relativesMap.containsKey(idenNo))
			{//如果是民政发放资金，但是又是公职人员亲属，视为异常
				outputKey.set(idenNo);
				outputValue.set(sendMoney);
				context.write(outputKey, outputValue);
				return;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
