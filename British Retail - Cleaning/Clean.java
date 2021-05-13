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

public class Clean {
    public static void main(String[] args) throws Exception {
        Job j = new Job();

        j.setJarByClass(Clean.class);
        j.setJobName("Clean");
        j.setNumReduceTasks(1);
        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[1]));

        j.setMapperClass(CleanMapper.class);
        j.setReducerClass(CleanReducer.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
    public static class CleanMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final int MISSING = 9999;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if(key.get() == 0)
                return;
            String line = value.toString();
            String[] lineData = line.split(",");

            if(lineData.length != 8 || Integer.parseInt(lineData[3]) < 0)
                return;

            IntWritable outputValue = new IntWritable(1);
            Text outputKey;
            outputKey = new Text(lineData[0] + "," + lineData[2] + "," +lineData[3] + "," +lineData[4] + "," +lineData[5] + "," +lineData[6]);

            context.write(outputKey,outputValue);
        }
    }
    public static class CleanReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, null);
        }
    }
}
