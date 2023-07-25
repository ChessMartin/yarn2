package com.opstty.job;

import com.opstty.mapper.TallestTreePerKindMapper;
import com.opstty.reducer.TallestTreePerKindReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TallestTreePerKind {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Job <inputPath> <outputPath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Tallest Tree By Species");
        job.setJarByClass(TallestTreePerKind.class);
        job.setMapperClass(TallestTreePerKindMapper.class);
        job.setReducerClass(TallestTreePerKindReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

