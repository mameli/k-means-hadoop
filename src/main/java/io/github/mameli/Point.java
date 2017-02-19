package io.github.mameli;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Created by mameli on 17/02/2017.
 * Point class
 */
public class Point implements WritableComparable<Point> {

    private DoubleWritable x = new DoubleWritable(0.0);

    private DoubleWritable y = new DoubleWritable(0.0);

    Point(DoubleWritable x, DoubleWritable y) {
        this.x = x;
        this.y = y;
    }

    Point() {
        this.x = new DoubleWritable(0.0);

        this.y = new DoubleWritable(0.0);
    }

    public void readFields(DataInput dataInput) throws IOException {
        x = new DoubleWritable(dataInput.readDouble());
        y = new DoubleWritable(dataInput.readDouble());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(getX().get());
        dataOutput.writeDouble(getY().get());
    }

    public String toString() {
        return this.x + " \t" + this.y;
    }

    public int compareTo(@Nonnull Point p) {
        if (p.getX().compareTo(this.x) == 0 && p.getY().compareTo(this.y) == 0) {
            return 0;
        }
        return -1;
    }

    DoubleWritable getX() {
        return x;
    }

    DoubleWritable getY() {
        return y;
    }

    void setX(DoubleWritable x) {
        this.x = x;
    }

    void setY(DoubleWritable y) {
        this.y = y;
    }
}
