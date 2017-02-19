package io.github.mameli;

import org.apache.hadoop.io.DoubleWritable;

/**
 * Created by mameli on 18/02/2017.
 * Center of k means
 */
public class Center extends Point {

    private int index;

    Center(DoubleWritable x, DoubleWritable y) {
        setX(x);
        setY(y);
    }

    Center() {
        setX(new DoubleWritable(0.0));
        setY(new DoubleWritable(0.0));
    }

    void setIndex(int index) {
        this.index = index;
    }


    public int getIndex() {
        return index;
    }
}
