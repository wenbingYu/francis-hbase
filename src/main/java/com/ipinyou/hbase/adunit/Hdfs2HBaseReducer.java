package com.ipinyou.hbase.adunit;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

public class Hdfs2HBaseReducer extends
		TableReducer<Text, Text, ImmutableBytesWritable> {

	public void reduce(Text rowkey, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {

		String hbaseRowkey = rowkey.toString();

		for (Text val : value) {
			Put put = new Put(hbaseRowkey.getBytes());
			String[] strs = val.toString().split(":::");
			String family = strs[0];
			String qualifier = strs[1];
			String valueDate = strs[2];
			put.add(family.getBytes(), qualifier.getBytes(),
					valueDate.getBytes());
			context.write(new ImmutableBytesWritable(hbaseRowkey.getBytes()),
					put);
		}

	}

}
