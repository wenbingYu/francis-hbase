package com.ipinyou.hbase.adunit;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ipinyou.hbase.utils.UrlCommonUtils;

public class Hdfs2HBaseMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text text, Context context) {
		String[] line = text.toString().split("\t");
		if (line.length == 99) {
			String actionRequestTime = line[6];
			String actionPlatform = line[3];
			String domain = UrlCommonUtils.getDomain(line[21]);
			String idAdvertiserCompanyId = line[64];
			String idAdUnitId = line[62];
			String adUnitWidth = line[66];
			String adUnitHeight = line[67];
			String adUnitFloorPrice = line[71];
			String payBidPrice = line[90];
			String payWinPrice = line[91];
			try {
				context.write(new Text(domain + actionRequestTime), new Text(
						"add" + ":::" + "idAdUnitId" + ":::" + idAdUnitId));
				context.write(new Text(domain + actionRequestTime), new Text(
						"add" + ":::" + "adUnitWidth" + ":::" + adUnitWidth));
				context.write(new Text(domain + actionRequestTime), new Text(
						"add" + ":::" + "adUnitHeight" + ":::" + adUnitHeight));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

}
