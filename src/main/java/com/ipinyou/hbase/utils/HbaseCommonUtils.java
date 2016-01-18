package com.ipinyou.hbase.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author wenbing.yu
 * @version V1.0
 * @function hbase curd operation
 * 
 * */

public class HbaseCommonUtils {

	// 声明静态配置 HBaseConfiguration
	static Configuration conf = null;

	static {
		conf = HBaseConfiguration.create();
	}

	// 创建一张表，通过HBaseAdmin HTableDescriptor来创建
	public static void creat(String tablename, String[] columnFamily)
			throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			System.out.println("table Exists!");
			System.exit(0);
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tablename);

			for (int i = 0; i < columnFamily.length; i++) {

				tableDesc.addFamily(new HColumnDescriptor(columnFamily[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create table success!");
		}
	}

	// 添加一条数据，通过HTable Put为已经存在的表来添加数据
	public static void put(String tablename, String row, String columnFamily,
			String column, String data) throws Exception {
		HTable table = new HTable(conf, tablename);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				Bytes.toBytes(data));
		table.put(p1);
		System.out.println("put '" + row + "','" + columnFamily + ":" + column
				+ "','" + data + "'");
	}

	public static Result get(String tablename, String rowkey)
			throws IOException {
		HTable table = new HTable(conf, tablename);
		Get g = new Get(Bytes.toBytes(rowkey));
		Result result = table.get(g);
		// System.out.println("Get: " + result);
		return result;
	}

	// 根据指定的表名获取数据
	public static ResultScanner scan(String tablename) throws Exception {
		HTable table = new HTable(conf, tablename);
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		return rs;

	}

	public static void getResultScann(String tableName) throws IOException {
		Scan scan = new Scan();
		ResultScanner rs = null;
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		try {
			rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue kv : r.list()) {
					System.out.println("row:" + Bytes.toString(kv.getRow()));
					System.out.println("family:"
							+ Bytes.toString(kv.getFamily()));
					System.out.println("qualifier:"
							+ Bytes.toString(kv.getQualifier()));
					System.out
							.println("value:" + Bytes.toString(kv.getValue()));
					System.out.println("timestamp:" + kv.getTimestamp());
					System.out
							.println("-------------------------------------------");
				}
			}
		} finally {
			rs.close();
		}
	}

	// 删除表
	public static boolean delete(String tablename) throws IOException {

		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			try {
				admin.disableTable(tablename);
				admin.deleteTable(tablename);
			} catch (Exception ex) {
				ex.printStackTrace();
				return false;
			}

		}
		return true;
	}
	
	
	public static void main(String[] args) {
		
		try {
			HbaseCommonUtils.creat("adunit", new String[]{"add"});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
