package io.github.mameli;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by mameli on 17/02/2017.
 * Point class
 */
public class Point implements WritableComparable<Point> {

    private List<DoubleWritable> listOfParameters;

    Point(List<DoubleWritable> listOfParameters) {
        this.listOfParameters = new ArrayList<DoubleWritable>();
        for (DoubleWritable p : listOfParameters) {
            this.listOfParameters.add(p);
        }
    }

    Point() {
        listOfParameters = new ArrayList<DoubleWritable>();
    }

    Point(int n) {
        listOfParameters = new ArrayList<DoubleWritable>();
        for (int i = 0; i < n; i++)
            listOfParameters.add(new DoubleWritable(0.0));
    }

    public void readFields(DataInput dataInput) throws IOException {
        int iParams = dataInput.readInt();
        listOfParameters = new ArrayList<DoubleWritable>();
        for (int i = 0; i < iParams; i++) {
            listOfParameters.add(new DoubleWritable(dataInput.readDouble()));
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(listOfParameters.size());
        for (DoubleWritable p : listOfParameters) {
            dataOutput.writeDouble(p.get());
        }
    }

    public String toString() {
        String elements = "";
        for (DoubleWritable e : listOfParameters) {
            elements += e.get() + ";";
        }
        return elements;
    }

    public int compareTo(@Nonnull Point p) {
        int isEqual = 0;
        for (int i = 0; i < listOfParameters.size(); i++) {
            if (p.getListOfParameters().get(i).compareTo(this.listOfParameters.get(i)) != 0)
                isEqual = 1;
        }
        return isEqual;
    }


    public List<DoubleWritable> getListOfParameters() {
        return listOfParameters;
    }

}
