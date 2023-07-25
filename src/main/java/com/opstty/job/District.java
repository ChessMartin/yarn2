package com.opstty.job;

import com.opstty.mapper.DistrictMapper;
import com.opstty.reducer.DistrictReducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class District {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DistrictJob <inputPath> <outputPath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "District Distinct Arrondissements");
        job.setJarByClass(District.class);
        job.setMapperClass(DistrictMapper.class);
        job.setReducerClass(DistrictReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

