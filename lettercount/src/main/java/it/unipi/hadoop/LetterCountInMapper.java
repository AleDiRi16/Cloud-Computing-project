package it.unipi.hadoop;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class LetterCountInMapper {

    // IN MAPPER COMBINING
    public static class LetterCountMapper extends Mapper<Object, Text, Text, LongWritable>{

        private Map<Text, Long> letterMap;

        protected void setup(Context context) {
            letterMap = new HashMap<Text, Long>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String text = value.toString().toLowerCase();
            
            for(char c : text.toCharArray()) {
                if (Character.isLetter(c)) {
                    Text letter = new Text(Character.toString(c));
                    letterMap.put(letter, letterMap.getOrDefault(letter, 0L) + 1);
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (Map.Entry<Text, Long> entry : letterMap.entrySet()) { 
                context.write(entry.getKey(), new LongWritable(entry.getValue()));
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
