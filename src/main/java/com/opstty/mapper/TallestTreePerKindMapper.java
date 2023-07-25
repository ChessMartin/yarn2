package com.opstty.mapper;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TallestTreePerKindMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        if (fields.length >= 7 && !fields[6].isEmpty()) {
            String species = fields[3].trim();
            double height = Double.parseDouble(fields[6]);
            context.write(new Text(species), new DoubleWritable(height));
        }
    }
}

