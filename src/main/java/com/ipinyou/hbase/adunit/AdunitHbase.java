package com.ipinyou.hbase.adunit;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ipinyou.hbase.utils.UrlCommonUtils;

public class AdunitHbase extends Configured implements Tool {

	public static class AdunitHbaseMapper<K> extends
			Mapper<LongWritable, Text, K, Put> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
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

				byte[] rowKey = (actionRequestTime + domain).getBytes();
				Put p = new Put(rowKey);
				p.add("add".getBytes(), "idAdUnitId".getBytes(),
						Bytes.toBytes(idAdUnitId));
				p.add("add".getBytes(), "adUnitWidth".getBytes(),
						Bytes.toBytes(adUnitWidth));
				p.add("add".getBytes(), "adUnitHeight".getBytes(),
						Bytes.toBytes(adUnitHeight));

				context.write(null, p);
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: HBaseTemperatureImporter <input>");
			return -1;
		}
		Job job = new Job(getConf(), getClass().getSimpleName());
		job.setJarByClass(getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,
				"observations");
		job.setMapperClass(AdunitHbaseMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TableOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(),
				new AdunitHbase(), args);
		System.exit(exitCode);
	}

}
