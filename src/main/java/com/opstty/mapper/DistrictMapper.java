package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class DistrictMapper extends Mapper<Object, Text, Text, NullWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        if (fields.length >= 2) {
            String arrondissement = fields[1].trim();
            context.write(new Text(arrondissement), NullWritable.get());
        }
    }
}

