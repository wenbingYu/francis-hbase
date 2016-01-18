package com.ipinyou.tool.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseMapper extends Configured implements Tool {

	private static Logger logger = LoggerFactory.getLogger(HbaseMapper.class);

	public static class MapperClass extends TableMapper<Text, NullWritable> {

		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws InterruptedException, IOException {

			String family = context.getConfiguration().get("family");

			String url = context.getConfiguration().get("url");

			String title_seg = context.getConfiguration().get("title_seg");

			String content_seg = context.getConfiguration().get("content_seg");

			for (int i = 0; i < value.size(); i++) {

				String segurl = null;
				String segtitle_seg = null;
				String segcontent_seg = null;
				Boolean flag_url = false;
				Boolean flag_title = false;
				Boolean falg_content = false;

				logger.info("map=============family======" + family
						+ "=========url=========" + url
						+ "==========title_seg===========" + title_seg
						+ "========content_seg==========" + content_seg);

				if (value != null) {
					try {
						if (!"".equals(family) && family != null) {

							logger.info("family=============" + family);

							if (!"".equals(url) && url != null) {
								segurl = new String(value.getValue(
										Bytes.toBytes(family),
										Bytes.toBytes(url)));
								flag_url = true;

								logger.info("url============" + segurl);

							}

							if (!"".equals(title_seg) && title_seg != null) {
								segtitle_seg = new String(value.getValue(
										Bytes.toBytes(family),
										Bytes.toBytes(title_seg)));
								flag_title = true;
								logger.info("title_seg============" + title_seg);
							}

							if (!"".equals(content_seg) && content_seg != null) {
								segcontent_seg = new String(value.getValue(
										Bytes.toBytes(family),
										Bytes.toBytes(content_seg)));
								falg_content = true;
								logger.info("content_seg============"
										+ content_seg);
							}

							if (flag_url && flag_title && falg_content) {
								context.write(
										new Text(segurl + "\t" + segtitle_seg
												+ "\t" + segcontent_seg),
										NullWritable.get());
								logger.info("999999999999999999999999999999");
							} else if (flag_url && flag_title) {
								context.write(new Text(segurl + "\t"
										+ segtitle_seg), NullWritable.get());
								logger.info("8888888888888888888888888888888");
							} else if (flag_url && falg_content) {
								context.write(new Text(segurl + "\t"
										+ segcontent_seg), NullWritable.get());
								logger.info("7777777777777777777777777777777777");
							} else if (flag_title && falg_content) {
								context.write(new Text(segtitle_seg + "\t"
										+ segcontent_seg), NullWritable.get());
								logger.info("6666666666666666666666666666666666666");

							} else if (flag_url) {

								context.write(new Text(segurl),
										NullWritable.get());
								logger.info("55555555555555555555555555555555555555");
							} else if (flag_title) {
								context.write(new Text(segtitle_seg),
										NullWritable.get());
								logger.info("444444444444444444444444444444444444");
							} else if (falg_content) {

								context.write(new Text(segcontent_seg),
										NullWritable.get());
								logger.info("333333333333333333333333333333333333");
							}

						}

					} catch (Exception e) {

						logger.info("################url#################################");
						logger.info(segurl);
						logger.info("################title_seg#################################");
						logger.info(segtitle_seg);
						logger.info("#################content_seg################################");
						logger.info(segcontent_seg);
						logger.info("#################################################");
						logger.info("#################################################");
						logger.info(e.toString());
					}
				}

			}
		}
	}

	public int run(String[] arg0) throws Exception {

		String target = this.getConf().get("target");

		Configuration conf = HBaseConfiguration.create();

		conf.set("hbase.zookeeper.quorum",
				"192.168.145.107,192.168.145.108,192.168.145.109");
		conf.set("mapreduce.job.queuename", "mapreduce.normal");

		Job job = Job.getInstance(this.getConf());
		job.setJarByClass(getClass());

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob("webpage", scan,
				MapperClass.class, Text.class, NullWritable.class, job);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(target));
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {

		String family = "";

		String url = "";

		String title_seg = "";

		String content_seg = "";

		String target = "";

		for (int i = 0; i < args.length; i++) {
			if ("-f".equals(args[i])) {
				family = args[++i];
			} else if ("-u".equals(args[i])) {
				url = args[++i];
			} else if ("-t".equals(args[i])) {
				title_seg = args[++i];
			} else if ("-c".equals(args[i])) {
				content_seg = args[++i];
			} else if ("-p".equals(args[i])) {
				target = args[++i];
			}
		}

		logger.info("main===============family======" + family
				+ "=========url=========" + url
				+ "==========title_seg===========" + title_seg
				+ "========content_seg==========" + content_seg
				+ "=========target==========" + target);

		Configuration conf = new Configuration();

		conf.set("family", family);
		conf.set("url", url);
		conf.set("title_seg", title_seg);
		conf.set("content_seg", content_seg);
		conf.set("target", target);

		conf.set("mapreduce.job.queuename", "mapreduce.normal");

		int res = ToolRunner.run(conf, new HbaseMapper(), args);
		System.exit(res);
	}

}
