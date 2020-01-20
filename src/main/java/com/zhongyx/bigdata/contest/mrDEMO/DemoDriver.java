package com.zhongyx.bigdata.contest.mrDEMO;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhongyx.bigdata.contest.demo1.Demo1Mapper;
import com.zhongyx.bigdata.contest.demo1.Demo1Reducer;

public class DemoDriver {

	//按年度统计每个年度的资金发放量（5分）
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 1 获取配置信息以及封装任务
		Configuration configuration = new Configuration();
		//设置切片大小为128MB
		configuration.setLong("mapreduce.input.fileinputformat.split.minsize",128*1024*1024);
		Job job = Job.getInstance(configuration);

		
		// 2 设置jar加载路径
		job.setJarByClass(DemoDriver.class);
		
		// 3 设置map和reduce类
		job.setMapperClass(Demo1Mapper.class);
		job.setReducerClass(Demo1Reducer.class);
		// 设置combiner
		job.setCombinerClass(Demo1Reducer.class);
		
		// 4 设置map输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		// 5 设置最终输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// 6 设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path("E:\\资料\\高新区比赛\\2018\\money_send.csv"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\资料\\高新区比赛\\2018\\mrResult\\demo1"));


		// 7 提交
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);

				
	}

}
