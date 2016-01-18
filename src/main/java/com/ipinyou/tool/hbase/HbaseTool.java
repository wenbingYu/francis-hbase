package com.ipinyou.tool.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTool {

	private Configuration conf;

	private HBaseAdmin admin;

	public void init() throws IOException {

		this.conf = HBaseConfiguration.create();
		admin = new HBaseAdmin(conf);

	}

	public HbaseTool() throws IOException {
		super();

		this.init();
	}

	/**
	 * 创建一张表
	 */
	public void creatTable(String tableName, String[] familys) throws Exception {

		if (admin.tableExists(tableName)) {
			System.out.println("table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(
					TableName.valueOf(tableName));
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create table " + tableName + " ok.");
		}

	}

	/**
	 * 删除表
	 */
	public void deleteTable(String tableName) throws Exception {
		try {

			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("delete table " + tableName + " ok.");
			admin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 插入一行记录
	 */
	public void addRecord(String tableName, String rowKey, String family,
			String qualifier, String value) throws Exception {
		try {
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
			System.out.println("insert recored " + rowKey + " to table "
					+ tableName + " ok.");
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 删除一行记录
	 */
	public void delRecord(String tableName, String rowKey) throws IOException {
		HTable table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
		table.close();
		System.out.println("del recored " + rowKey + " ok.");
	}

	/**
	 * 查找一行记录
	 */
	public Result getOneRecord(String tableName, String rowKey)
			throws IOException {
		HTable table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);

		table.close();
		return rs;
	}

	public Result getOneRecord(String tableName, String rowKey, String family,
			String qualifier) throws IOException {
		HTable table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());

		if (family != null && !family.isEmpty()) {
			if (qualifier != null && !qualifier.isEmpty()) {
				get.addColumn(family.getBytes(), qualifier.getBytes());
			} else {
				get.addFamily(family.getBytes());
			}
		}

		Result rs = table.get(get);
		table.close();
		return rs;
	}

	/**
	 * 显示所有数据
	 * 
	 * @throws IOException
	 */
	public ResultScanner getAllRecord(String tableName) throws IOException {

		HTable table = new HTable(conf, tableName);
		Scan s = new Scan();
		ResultScanner ss = table.getScanner(s);
		table.close();
		return ss;
	}

	public void printResult(Result rs) {
		for (Cell c : rs.rawCells()) {
			StringBuilder sb = new StringBuilder();
			sb.append(Bytes.toString(CellUtil.cloneRow(c))).append(" ");
			sb.append(Bytes.toString(CellUtil.cloneFamily(c))).append(":")
					.append(Bytes.toString(CellUtil.cloneQualifier(c)))
					.append(" ").append(Long.toString(c.getTimestamp()))
					.append(" ").append(Bytes.toString(CellUtil.cloneValue(c)));

			System.out.println(sb.toString());
		}
	}

	public void printResultScanner(ResultScanner rss) {
		for (Result r : rss) {
			for (Cell c : r.rawCells()) {
				StringBuilder sb = new StringBuilder();
				sb.append(Bytes.toString(CellUtil.cloneRow(c))).append(" ");
				sb.append(Bytes.toString(CellUtil.cloneFamily(c))).append(":")
						.append(Bytes.toString(CellUtil.cloneQualifier(c)))
						.append(" ").append(Long.toString(c.getTimestamp()))
						.append(" ")
						.append(Bytes.toString(CellUtil.cloneValue(c)));

				System.out.println(sb.toString());
			}
		}
	}

	public static void main(String[] agrs) {
		try {
			String tablename = "scores_new";
			String[] familys = { "grade", "course" };
			HbaseTool hop = new HbaseTool();
			hop.creatTable(tablename, familys);

			// add record tom
			hop.addRecord(tablename, "tom", "grade", "", "5");
			hop.addRecord(tablename, "tom", "course", "", "90");
			hop.addRecord(tablename, "tom", "course", "math", "97");
			hop.addRecord(tablename, "tom", "course", "art", "87");
			// add record bob
			hop.addRecord(tablename, "bob", "grade", "", "4");
			hop.addRecord(tablename, "bob", "course", "math", "89");

			System.out.println("===========get one record========");
			Result rs = hop.getOneRecord(tablename, "tom");
			hop.printResult(rs);

			System.out.println("===========get one record one column========");
			rs = hop.getOneRecord(tablename, "tom", "course", "art");
			hop.printResult(rs);

			System.out.println("===========show all record========");
			ResultScanner rss = hop.getAllRecord(tablename);
			hop.printResultScanner(rss);
			System.out.println("===========del one record bob========");
			hop.delRecord(tablename, "bob");

			System.out.println("===========show all record========");
			rss = hop.getAllRecord(tablename);
			hop.printResultScanner(rss);

			System.out.println("===========del one record tom========");
			hop.delRecord(tablename, "tom");
			System.out.println("===========show all record========");
			rss = hop.getAllRecord(tablename);
			hop.printResultScanner(rss);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
