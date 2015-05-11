package comps;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CommonComps {

	public static class MapA extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		public void map(LongWritable key, Text input, Context context) throws IOException, InterruptedException {
			StringTokenizer toke = new StringTokenizer(input.toString(), "\n");
			while (toke.hasMoreTokens()) {
				String line = toke.nextToken().toString();
				StringTokenizer lineToke = new StringTokenizer(line, "}");
				while (lineToke.hasMoreTokens()){
					String entry = lineToke.nextToken();
					StringTokenizer entryToke = new StringTokenizer(entry, ",");
					while (entryToke.hasMoreTokens()){
						String temp = entryToke.nextToken().toString();
						if (temp.contains("what")){
							String[] split = temp.split(":");
							context.write(new Text(split[1].replace("\"", "")), new IntWritable(1));
						}
					}
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			//Just need to sum the the number of occurrences together.
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(new Text("INSERT INTO 'component_count' values ('" + key +  "', " + sum + ");"), new Text(""));
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "commonComps");
		job.setJarByClass(CommonComps.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MapA.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}