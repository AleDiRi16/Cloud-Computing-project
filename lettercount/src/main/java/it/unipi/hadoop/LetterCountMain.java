package it.unipi.hadoop;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import it.unipi.hadoop.LetterCount;
import it.unipi.hadoop.LetterCountInMapper;
import it.unipi.hadoop.LetterCountNoCombiner;

public class LetterCountMain {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // check if all parameters are corrected inserted
        if (otherArgs.length != 4) {
            System.err.println("Usage: LetterCountMain <input> <outoput> <reducer number> <combining strategy>");
            System.exit(1);
        }

        // check if at least 1 reducer is inserted
        int reducersNumber = Integer.parseInt(otherArgs[2]);
        if (reducersNumber < 1) {
            System.err.println("Wrong reducer number, (at least 1)");
            System.exit(1);
        }

        // check if a valid combinar strategy is selected
        int combStrategy = Integer.parseInt(otherArgs[3]);
        if(combStrategy < 0 || combStrategy > 2){
            System.err.println("Wrong combiner strategy, (range 0-2)");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "LetterCount");
	    job.setInputFormatClass(CombineTextInputFormat.class);

        // set mapper/reducer based on the input parameter
        // no combiner
        if(combStrategy == 0){
            job.setJarByClass(LetterCountNoCombiner.class);
            job.setMapperClass(LetterCountNoCombiner.LetterCountMapper.class);
            job.setReducerClass(LetterCountNoCombiner.LetterCountReducer.class);
        // in mapping
        } else if (combStrategy == 1) {
            job.setJarByClass(LetterCountInMapper.class);
            job.setMapperClass(LetterCountInMapper.LetterCountMapper.class); // In-Mapper combining
            job.setReducerClass(LetterCountInMapper.LetterCountReducer.class);
        // combiner
        } else {
            job.setJarByClass(LetterCount.class);
            job.setMapperClass(LetterCount.LetterCountMapper.class);
            job.setCombinerClass(LetterCount.LetterCountCombiner.class);
            job.setReducerClass(LetterCount.LetterCountReducer.class);
        }

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // define reducers number based on input parameter
        job.setNumReduceTasks(reducersNumber);

        // save time stats to csv
        long startTime = System.currentTimeMillis();

        if (job.waitForCompletion(true)) {
            double elapsedTime = (System.currentTimeMillis() - startTime) / 1000;
            writeToCSV(otherArgs[0], elapsedTime, reducersNumber, combStrategy);
            System.exit(0);
        } else {
            System.err.println("Job error");
            System.exit(1);
        }
    }

    // Method to save statistics in a CSV file
    public static void writeToCSV(String filename, double elapsedTime, int reducersNumber, int combStrategy) {
        String csvFile = "stats.csv";
        String csvHeader = "Filename, elapsedTime, reducersNumber, combStrategy";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile, true))) {
            // check if empty file
            if (new java.io.File(csvFile).length() == 0) {
                writer.write(csvHeader);
                writer.newLine();
            }
            writer.write(filename + "," + elapsedTime + "," + reducersNumber + "," + combStrategy);
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Error writing to CSV: " + e.getMessage());
        }
    }
}

