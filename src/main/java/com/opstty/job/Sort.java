package com.opstty.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.opstty.HeightSpeciesComparator;
import com.opstty.HeightSpeciesWritable;
import com.opstty.mapper.SortMapper;
import com.opstty.reducer.SortReducer;

public class Sort {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Job <inputPath> <outputPath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sort Trees by Height");
        job.setJarByClass(Sort.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setMapOutputKeyClass(HeightSpeciesWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(HeightSpeciesComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

