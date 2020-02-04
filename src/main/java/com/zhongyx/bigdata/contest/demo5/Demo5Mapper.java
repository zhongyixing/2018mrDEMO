package com.zhongyx.bigdata.contest.demo5;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//（5）分析违规发放情况，发现违法违纪重灾区，如：易出现问题的资金、人群、年度、性别等为领导决策提供参考。(10)
public class Demo5Mapper extends Mapper<LongWritable, Text, Demo5Bean, DoubleWritable>{

	Demo5Bean outputKey = new Demo5Bean();
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
	protected void setup(Mapper<LongWritable, Text, Demo5Bean, DoubleWritable>.Context context)
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
			//System.out.println("break:"+line);
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
//			System.out.println("dead:"+line);
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
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Demo5Bean, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		try
		{
			boolean exFlag=false;
			List<String> exList=new ArrayList<String>();
			
			String line=value.toString();
			
			String[] columns=line.split("\t");
			
			String idenNo=columns[0];
			
			String dept=columns[1];
			
			String moneyType=columns[2];
			
			int gender=Integer.parseInt(columns[3]);
			
			String sendDate=columns[4];
			
			int year=Integer.parseInt(columns[4].substring(0,4));
			
			Double sendMoney=Double.valueOf(columns[5]);
			
			if(deadManMap.containsKey(idenNo))
			{//如果是死亡人员
				long deadTime=deadManMap.get(idenNo);
				
				SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
				long sendTime=sdf.parse(sendDate).getTime();
				
				if((deadTime+(1000*60*60*24*180))>sendTime)
				{//死亡时间180天后，仍然有资金发放，视为异常
					exFlag=true;
					exList.add("死亡人群");
				}
			}
			
			if(dept.equals("残联") && !breakManMap.containsKey(idenNo))
			{//如果是残联发放资金，但是在残疾人表找不到身份证号，视为异常
				exFlag=true;
				exList.add("残疾人群");
			}
			
			if(masterMap.containsKey(idenNo))
			{//如果是户主的公职人员，视为异常
				exFlag=true;
				exList.add("公职人群");
			}
			
			if(dept.equals("民政") && relativesMap.containsKey(idenNo))
			{//如果是民政发放资金，但是又是公职人员亲属，视为异常
				exFlag=true;
				exList.add("公职亲属人群");
			}
			
			if(exFlag)
			{
				String exDesc=StringUtils.join(exList.toArray(),",");
				Demo5Bean dimkey=new Demo5Bean(moneyType,year,gender,exDesc);
				
				outputValue.set(sendMoney);
				
				context.write(dimkey, outputValue);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
