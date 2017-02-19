package io.github.mameli;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by mameli on 18/02/2017.
 * Center of k means
 */
public class Center extends Point {

    private IntWritable index = new IntWritable(0);

    private IntWritable numberOfPoints = new IntWritable(0);

    Center(DoubleWritable x, DoubleWritable y, IntWritable index, IntWritable numberOfPoints) {
        super(x, y);
        this.index = new IntWritable(index.get());
        this.numberOfPoints = new IntWritable(numberOfPoints.get());
    }

    Center(DoubleWritable x, DoubleWritable y) {
        setX(x);
        setY(y);
    }

    Center() {
        setX(new DoubleWritable(0.0));
        setY(new DoubleWritable(0.0));
    }

    Center(Center c) {
        setX(c.getX());
        setY(c.getY());
        setNumberOfPoints(c.getNumberOfPoints());
        setIndex(c.getIndex());
    }

    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        index = new IntWritable(dataInput.readInt());
        numberOfPoints = new IntWritable(dataInput.readInt());
    }

    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeInt(index.get());
        dataOutput.writeInt(numberOfPoints.get());
    }

    public String toString() {
        return "index: " + this.getIndex() + " " + super.toString() + "  n: " + numberOfPoints;
    }

    IntWritable getIndex() {
        return index;
    }

    IntWritable getNumberOfPoints() {
        return numberOfPoints;
    }

    private void setIndex(IntWritable index) {
        this.index = new IntWritable(index.get());
    }

    void setNumberOfPoints(IntWritable numberOfPoints) {
        this.numberOfPoints = new IntWritable(numberOfPoints.get());
    }
}
