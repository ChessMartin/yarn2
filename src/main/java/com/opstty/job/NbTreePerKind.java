package com.opstty.job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import com.opstty.mapper.NbTreePerKindMapper;
import com.opstty.reducer.NbTreePerKindReducer;

public class NbTreePerKind {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Job <inputPath> <outputPath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Tree Species Count");
        job.setJarByClass(NbTreePerKind.class);
        job.setMapperClass(NbTreePerKindMapper.class);
        job.setReducerClass(NbTreePerKindReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

