package com.ipinyou.tool.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hello world!
 *
 */
public class PureCleanAll extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] line = value.toString().split("\t");

			if (line != null && line.length >= 2) {

				String AgentUrl = line[1];

				String pyid = line[0];

				int p = AgentUrl.lastIndexOf("p=");

				int s = AgentUrl.lastIndexOf("&s=");

				if (p != -1 && s != -1) {

					String product = AgentUrl.substring(p + 2, s);

					if (!"".equals(product)) {

						String[] pro = product.split(",");

						if (pro.length > 0) {

							for (int i = 0; i < pro.length; i++) {
								context.write(new Text(pyid), new Text(pro[i]));
							}

						}

					}

				}
			}

		}
	}

	// public static class Reduce extends
	// Reducer<Text, IntWritable, Text, IntWritable> {
	// public void reduce(Text key, Iterable<IntWritable> val, Context context)
	// throws IOException, InterruptedException {
	// int sum = 0;
	// Iterator<IntWritable> values = val.iterator();
	// while (values.hasNext()) {
	// sum += values.next().get();
	// }
	// context.write(key, new IntWritable(sum));
	//
	// }
	// }

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf(), "eval");
		job.setJarByClass(getClass());
		// set up the input
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(arg0[0]));
		// Mapper
		job.setMapperClass(Map.class);
		// Reducer
		// job.setReducerClass(Reducer.class);

		// set up the output
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		TextOutputFormat.setOutputPath(job, new Path(arg0[1]));

		boolean res = job.waitForCompletion(true);
		if (res)
			return 0;
		else
			return -1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new PureCleanAll(), args);
		System.exit(res);
	}
}
