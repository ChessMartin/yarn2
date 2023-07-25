package com.opstty.reducer;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TallestTreePerKindReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double maxHeight = 0.0;
        for (DoubleWritable value : values) {
            double height = value.get();
            if (height > maxHeight) {
                maxHeight = height;
            }
        }
        context.write(key, new DoubleWritable(maxHeight));
    }
}

