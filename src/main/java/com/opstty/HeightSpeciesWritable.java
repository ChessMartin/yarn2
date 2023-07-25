package com.opstty;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.opstty.HeightSpeciesComparator;

public class HeightSpeciesWritable implements WritableComparable<HeightSpeciesWritable> {

    private DoubleWritable height;
    private Text species;

    public HeightSpeciesWritable() {
        height = new DoubleWritable();
        species = new Text();
    }

    public HeightSpeciesWritable(double height, String species) {
        this.height = new DoubleWritable(height);
        this.species = new Text(species);
    }

    public double getHeight() {
        return height.get();
    }

    public String getSpecies() {
        return species.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        height.write(out);
        species.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        height.readFields(in);
        species.readFields(in);
    }

    @Override
    public int compareTo(HeightSpeciesWritable other) {
        int heightComparison = height.compareTo(other.height);
        if (heightComparison != 0) {
            return heightComparison;
        }
        return species.compareTo(other.species);
    }

    @Override
    public String toString() {
        return height.toString() + "\t" + species.toString();
    }
}

