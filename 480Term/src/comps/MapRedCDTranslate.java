package comps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapRedCDTranslate {

	public static class MapD extends Mapper<Text, IntWritable, Text, IntWritable> {
		
		@Override
		public void map(Text key, IntWritable input, Context context) throws IOException, InterruptedException {
			context.write(key, input);
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			if (values != null){
				for (IntWritable i : values) {
					context.write(new Text("INSERT INTO 'component_per_date' values ('" + key.toString().trim()), new Text("', " + i.get() + ");"));
				}
			}
		}
	}

	@SuppressWarnings("deprecation")
	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "commonDatesTrans");
		job.setJarByClass(MapRedCDTranslate.class);
		
		FileInputFormat.addInputPath(job, new Path("s3://480term/cd/"));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(MapD.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}