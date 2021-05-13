import java.io.IOException;
import java.util.ArrayList;

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

public class PopularProducts {
    public static void main(String [] args) throws Exception
    {
        Job j=new Job();

        j.setJarByClass(PopularProducts.class);
        j.setJobName("PopularProducts");
        j.setNumReduceTasks(1);
        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[1]));

        j.setMapperClass(PopularProductsMapper.class);
        j.setReducerClass(PopularProductsReducer.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
    public static class PopularProductsMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final int MISSING = 9999;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if(key.get() == 0)
                return;

            String line = value.toString();
            String[] words = line.split(",");

            Text outputKey = new Text(words[1]);

            IntWritable outputValue = new IntWritable(Integer.valueOf(words[2]));
            context.write(outputKey,outputValue);
        }
    }

    public static class PopularProductsReducer
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