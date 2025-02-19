package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class LetterCountNoCombiner {

    public static class LetterCountMapper extends Mapper<Object, Text, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);
        private Text letter = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String text = value.toString().toLowerCase();

            for (char c : text.toCharArray()) {
                if (Character.isLetter(c)){
                    letter.set(Character.toString(c));
                    context.write(letter, one);
                }
            }
        }
    }

    public static class LetterCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long sum = 0;

            for (LongWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }
}
