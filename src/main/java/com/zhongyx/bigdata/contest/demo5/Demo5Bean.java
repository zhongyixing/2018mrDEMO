package com.zhongyx.bigdata.contest.demo5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Demo5Bean implements WritableComparable<Demo5Bean>{

	private String moneyType;
	
	private int year;
	
	private int gender;
	
	private String exDesc;
	
	public Demo5Bean() {
		super();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(moneyType);
		out.writeInt(year);
		out.writeInt(gender);
		out.writeUTF(exDesc);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.moneyType=in.readUTF();
		this.year=in.readInt();
		this.gender=in.readInt();
		this.exDesc=in.readUTF();
	}
	
	@Override
	public int compareTo(Demo5Bean o) {
		if(this.moneyType.compareTo(o.moneyType)>0)
		{
			return 1;
		}
		else if(this.moneyType.compareTo(o.moneyType)<0)
		{
			return -1;
		}
		else
		{
			
			if(this.exDesc.compareTo(o.exDesc)>0)
			{
				return 1;
			}
			else if(this.exDesc.compareTo(o.exDesc)<0)
			{
				return -1;
			}
			else
			{
				if(this.year>o.year)
				{
					return 1;
				}
				else if(this.year<o.year)
				{
					return -1;
				}
				else
				{
					if(this.gender>o.gender)
					{
						return 1;
					}
					else if(this.gender<o.gender)
					{
						return -1;
					}
					else
					{
						return 0;
					}
				}
			}
		}
	}

	public String getMoneyType() {
		return moneyType;
	}

	public void setMoneyType(String moneyType) {
		this.moneyType = moneyType;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getGender() {
		return gender;
	}

	public void setGender(int gender) {
		this.gender = gender;
	}

	public String getExDesc() {
		return exDesc;
	}

	public void setExDesc(String exDesc) {
		this.exDesc = exDesc;
	}

	public Demo5Bean(String moneyType, int year, int gender, String exDesc) {
		super();
		this.moneyType = moneyType;
		this.year = year;
		this.gender = gender;
		this.exDesc = exDesc;
	}

	@Override
	public String toString() {
		return "[" + moneyType + "-" + exDesc + "-" + year + "-" + gender+ "]";
	}
	
}
