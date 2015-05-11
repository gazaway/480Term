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
						split[1] = split[1].replace("\"what\":", "").trim();
						split[1] = split[1].replace("\"", "");
						if (split[1].contains("pdate")){
							split[1] = "Update";
						}
						context.write(new Text(split[0] + '\t' + split[1] + '\t'), new IntWritable(1));
					}
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			String[] keys = key.toString().trim().split("\t");
			//Just need to sum the the number of occurrences together.
			for (IntWritable val : values) {
				sum += val.get();
			}
			if (keys[1] != null) {
				context.write(new Text("INSERT INTO 'component_per_date' values ('" + keys[0].trim() + "', '" + keys[1].trim() +  "', " + sum + ");"), new Text(""));
			}
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
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}