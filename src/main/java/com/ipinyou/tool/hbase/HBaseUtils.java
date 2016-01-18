package com.ipinyou.tool.hbase;

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
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtils {
	// 声明静态配置 HBaseConfiguration
	static Configuration cfg = HBaseConfiguration.create();

	// 创建一张表，通过HBaseAdmin HTableDescriptor来创建
	public static void creat(String tablename, String columnFamily)
			throws Exception {
		HBaseAdmin admin = new HBaseAdmin(cfg);
		if (admin.tableExists(tablename)) {
			System.out.println("table Exists!");
			System.exit(0);
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tablename);
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println("create table success!");
		}
	}

	// 添加一条数据，通过HTable Put为已经存在的表来添加数据
	public static void put(String tablename, String row, String columnFamily,
			String column, String data) throws Exception {
		HTable table = new HTable(cfg, tablename);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				Bytes.toBytes(data));
		table.put(p1);
		System.out.println("put '" + row + "','" + columnFamily + ":" + column
				+ "','" + data + "'");
	}

	public static Result get(String tablename, String row) throws IOException {
		HTable table = new HTable(cfg, tablename);
		Get g = new Get(Bytes.toBytes(row));
		Result result = table.get(g);
		// System.out.println("Get: " + result);
		return result;
	}

	// 显示所有数据，通过HTable Scan来获取已有表的信息
	public static ResultScanner scan(String tablename) throws Exception {
		HTable table = new HTable(cfg, tablename);
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		return rs;

	}

	// public static List<Entity> list(String tablename, String columnFamily,
	// String column) throws Exception {
	// HTable table = new HTable(cfg, tablename);
	// Scan s = new Scan();
	// ResultScanner rs = table.getScanner(s);
	// // return rs;
	// List<Entity> listResult = new ArrayList<Entity>();
	// for (Result r : rs) {
	// // System.out.println("Scan: " + r);
	// Entity entity = new Entity();
	// byte[] val = r.getValue(Bytes.toBytes(columnFamily),
	// Bytes.toBytes(column));
	// entity.setField(column, Bytes.toString(val));
	// listResult.add(entity);
	// }
	// return listResult;
	//
	// }

	public static boolean delete(String tablename) throws IOException {

		HBaseAdmin admin = new HBaseAdmin(cfg);
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

	public static void getResultScann(String tableName) throws IOException {
		Scan scan = new Scan();
		ResultScanner rs = null;
		HTable table = new HTable(cfg, Bytes.toBytes(tableName));
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

	public static void getFilterDate(String tablename, String rowkey) {

		try {
			HTable table = new HTable(cfg, tablename);

			Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL,
					new SubstringComparator(rowkey));

			Scan s = new Scan();

			s.setFilter(filter1);

			ResultScanner rs = table.getScanner(s);

			for (Result r : rs) {
				System.out.println("rowkey:" + new String(r.getRow()));
				for (KeyValue keyvalue : r.raw()) {
					System.out.println("columb family:----"
							+ new String(keyvalue.getFamily()) + "column---"
							+ new String(keyvalue.getQualifier()) + "value===="
							+ new String(keyvalue.getValue()));
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] agrs) {
		String tablename = "ipinyou-adunit";
		String columnFamily = "attt";
		// System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop");
		// System.setProperty("HADOOP_USER_NAME", "mapred");

		HBaseUtils hbu = new HBaseUtils();

		getFilterDate(tablename, "20151202");

		// List<Entity> result = HBaseUtils.list("hbase_tb", "cf", "cl1");
		// for (Entity r : result) {
		// System.out.println(r.getField("cl1"));
		// }

		// try {
		// hbu.creat("test", "ctr");
		// } catch (Exception e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		// LineIterator it = null;
		//
		// try {
		//
		//
		// it = FileUtils.lineIterator(new File("D:\\format_count.txt"),
		// "UTF-8");
		//
		// while (it.hasNext()) {
		//
		// String[] line = it.next().split("\t");
		//
		// if (line.length > 12) {
		//
		// String rowkey = line[0];
		//
		// String IdAdUnitId = line[1];
		//
		// String AdUnitWidth = line[2];
		//
		// String AdUnitHeight = line[3];
		//
		// String AgentAppId = line[4];
		//
		// String Domain = line[5];
		//
		// String AdUnitFloorPrice = line[6];
		//
		// String PayBidPrice = line[7];
		//
		// String PayWinPrice = line[8];
		//
		// String IsBid = line[9];
		//
		// String IsImp = line[10];
		//
		// String IsClk = line[11];
		//
		// String IsCvt = line[12];
		//
		// hbu.put(tablename, rowkey, "attt", "IdAdUnitId", IdAdUnitId);
		// hbu.put(tablename, rowkey, "attt", "AdUnitWidth",
		// AdUnitWidth);
		// hbu.put(tablename, rowkey, "attt", "AdUnitHeight",
		// AdUnitHeight);
		// hbu.put(tablename, rowkey, "attt", "AgentAppId", AgentAppId);
		// hbu.put(tablename, rowkey, "attt", "Domain", Domain);
		// hbu.put(tablename, rowkey, "attt", "AdUnitFloorPrice",
		// AdUnitFloorPrice);
		// hbu.put(tablename, rowkey, "attt", "PayBidPrice",
		// PayBidPrice);
		// hbu.put(tablename, rowkey, "attt", "PayWinPrice",
		// PayWinPrice);
		// hbu.put(tablename, rowkey, "attt", "IsBid", IsBid);
		// hbu.put(tablename, rowkey, "attt", "IsImp", IsImp);
		// hbu.put(tablename, rowkey, "attt", "IsClk", IsClk);
		// hbu.put(tablename, rowkey, "attt", "IsCvt", IsCvt);
		//
		// }
		//
		// }
		//
		// // List<Entity> result = HBaseUtils.list("hbase_tb", "cf", "cl1");
		// // for (Entity r : result) {
		// // System.out.println(r.getField("cl1"));
		// // }
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
	}
}