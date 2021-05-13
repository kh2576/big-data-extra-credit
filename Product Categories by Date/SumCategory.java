import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SumCategory {
    public static void main(String[] args) throws Exception {
        Job j = new Job();

        j.setJarByClass(SumCategory.class);
        j.setJobName("SumCategory");
        j.setNumReduceTasks(1);
        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[1]));

        j.setMapperClass(SumCategoryMapper.class);
        j.setReducerClass(SumCategoryReducer.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
    public static class SumCategoryMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final int MISSING = 9999;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");

            String event_date = words[2];
            String category = words[5];

            context.write(new Text(event_date + "," + category +","),new IntWritable(1));
        }
    }
    public static class SumCategoryReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable value : values)
            {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
