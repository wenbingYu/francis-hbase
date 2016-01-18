package com.ipinyou.hbase.adunit;

import javax.ws.rs.PUT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class Hdfs2HBase {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] argsparams = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (argsparams.length != 1) {
			System.err.println("USage:Hdfs2HBase<input><output>");
			System.exit(2);
		}

		Job job = new Job(conf, "Hdfs2HBase-adunit");
		job.setJarByClass(Hdfs2HBase.class);
		job.setMapperClass(Hdfs2HBaseMapper.class);
		job.setReducerClass(Hdfs2HBaseReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(PUT.class);

		job.setOutputFormatClass(TableOutputFormat.class);
		System.out.println(argsparams[0]);
		FileInputFormat.addInputPath(job, new Path(argsparams[0]));
//		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,
//				argsparams[1]);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,"adunit");
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
