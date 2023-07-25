package com.opstty.mapper;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        if (fields.length >= 4) {
            String species = fields[3].trim();
            context.write(new Text(species), NullWritable.get());
        }
    }
}

