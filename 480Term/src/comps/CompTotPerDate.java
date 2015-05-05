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

public class CompTotPerDate {

	public static class MapB extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		public void map(LongWritable key, Text input, Context context) throws IOException, InterruptedException {
			StringTokenizer toke = new StringTokenizer(input.toString(), "\n");
			while (toke.hasMoreTokens()) {
				String line = toke.nextToken();
				StringTokenizer lineToke = new StringTokenizer(line, "}");
				while (lineToke.hasMoreTokens()){
					String entry = lineToke.nextToken();
					if (!entry.contains("]")){
						String[] split = entry.split("\\{");
						entry = split[1];
						split = entry.split(",");
						split[0] = split[0].replace("\"when\":", "");
						StringBuilder temp = new StringBuilder(split[0]);
						temp.replace(split[0].length()-2, split[0].length(), "XX");
						split[0] = temp.toString();
						split[1] = split[1].replace("\"what\":", "");
						split[1] = split[1].replace("\"", "");
						context.write(new Text(split[0] + " " + split[1]), new IntWritable(1));
					}
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			//Just need to sum the the number of occurrences together.
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "compTotPerDate");
		job.setJarByClass(CompTotPerDate.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MapB.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}