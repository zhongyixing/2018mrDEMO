package com.zhongyx.bigdata.contest.mrDEMO;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhongyx.bigdata.contest.demo2.Demo2Mapper;
import com.zhongyx.bigdata.contest.demo2.Demo2Reducer;

public class DemoDriver {

//	//按年度统计每个年度的资金发放量（5分）
//	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//		// 1 获取配置信息以及封装任务
//		Configuration configuration = new Configuration();
//		//设置切片大小为128MB
//		configuration.setLong("mapreduce.input.fileinputformat.split.minsize",128*1024*1024);
//		Job job = Job.getInstance(configuration);
//
//		
//		// 2 设置jar加载路径
//		job.setJarByClass(DemoDriver.class);
//		
//		// 3 设置map和reduce类
//		job.setMapperClass(Demo1Mapper.class);
//		job.setReducerClass(Demo1Reducer.class);
//		// 设置combiner
//		job.setCombinerClass(Demo1Reducer.class);
//		
//		// 4 设置map输出
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(DoubleWritable.class);
//
//		// 5 设置最终输出kv类型
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(DoubleWritable.class);
//
//		// 6 设置输入和输出路径
//		FileInputFormat.setInputPaths(job, new Path("E:\\资料\\高新区比赛\\2018\\input\\惠民资金发放信息.csv"));
//		FileOutputFormat.setOutputPath(job, new Path("E:\\资料\\高新区比赛\\2018\\mrOutput\\demo1"));
//
//
//		// 7 提交
//		boolean result = job.waitForCompletion(true);
//
//		System.exit(result ? 0 : 1);		
//	}
	
//	//（2）按年度统计各个部门资金总发放量（如2018-民政发放量，2017-民政发放量、教育等）（5分）
//	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//		// 1 获取配置信息以及封装任务
//		Configuration configuration = new Configuration();
//		//设置切片大小为128MB
//		configuration.setLong("mapreduce.input.fileinputformat.split.minsize",128*1024*1024);
//		Job job = Job.getInstance(configuration);
//
//		
//		// 2 设置jar加载路径
//		job.setJarByClass(DemoDriver.class);
//		
//		// 3 设置map和reduce类
//		job.setMapperClass(Demo2Mapper.class);
//		job.setReducerClass(Demo2Reducer.class);
//		// 设置combiner
//		job.setCombinerClass(Demo2Reducer.class);
//		
//		// 4 设置map输出
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(DoubleWritable.class);
//
//		// 5 设置最终输出kv类型
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(DoubleWritable.class);
//
//		// 6 设置输入和输出路径
//		FileInputFormat.setInputPaths(job, new Path("E:\\资料\\高新区比赛\\2018\\input\\惠民资金发放信息.csv"));
//		FileOutputFormat.setOutputPath(job, new Path("E:\\资料\\高新区比赛\\2018\\mrOutput\\demo2"));
//
//
//		// 7 提交
//		boolean result = job.waitForCompletion(true);
//
//		System.exit(result ? 0 : 1);		
//	}
	
//	//（3）按每个人合计领取资金总量分布统计人数，具体统计项：个人合计领取低于五千人数、五千至三万人数、三至五万人数、五至十万人数、十至二十万人数、二十万以上人数（10分）
//	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//		// 1 获取配置信息以及封装任务
//		Configuration configuration = new Configuration();
//		//设置切片大小为128MB
//		configuration.setLong("mapreduce.input.fileinputformat.split.minsize",128*1024*1024);
//		Job job = Job.getInstance(configuration);
//	
//		
//		// 2 设置jar加载路径
//		job.setJarByClass(DemoDriver.class);
//		
//		// 3 设置map和reduce类
//		job.setMapperClass(Demo3Mapper.class);
//		job.setReducerClass(Demo3Reducer.class);
//		// 设置combiner
//		job.setCombinerClass(Demo3Combiner.class);
//		
//		// 4 设置map输出
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(DoubleWritable.class);
//	
//		// 5 设置最终输出kv类型
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//	
//		// 6 设置输入和输出路径
//		FileInputFormat.setInputPaths(job, new Path("E:\\资料\\高新区比赛\\2018\\input\\惠民资金发放信息.csv"));
//		FileOutputFormat.setOutputPath(job, new Path("E:\\资料\\高新区比赛\\2018\\mrOutput\\demo3"));
//	
//	
//		// 7 提交
//		boolean result = job.waitForCompletion(true);
//	
//		System.exit(result ? 0 : 1);		
//	}

}
