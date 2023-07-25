package com.opstty;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HeightSpeciesComparator extends WritableComparator {

    protected HeightSpeciesComparator() {
        super(HeightSpeciesWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        HeightSpeciesWritable h1 = (HeightSpeciesWritable) a;
        HeightSpeciesWritable h2 = (HeightSpeciesWritable) b;
        return h1.compareTo(h2);
    }
}

