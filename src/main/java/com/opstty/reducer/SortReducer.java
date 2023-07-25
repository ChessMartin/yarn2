package com.opstty.reducer;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.opstty.HeightSpeciesWritable;

public class SortReducer extends Reducer<HeightSpeciesWritable, Text, Text, Text> {

    @Override
    public void reduce(HeightSpeciesWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(new Text(Double.toString(key.getHeight())), value);
        }
    }
}

