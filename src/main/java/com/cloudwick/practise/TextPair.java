package com.cloudwick.practise;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Created by Rajiv on 3/21/17.
 */
public class TextPair implements WritableComparable<TextPair> {


    private Text state;
    private Text city;


    // Mapreduce should be able to initialize the parameters
    public TextPair() {
        this.state = new Text();
        this.city = new Text();
    }

    public TextPair(Text city, Text state) {
        this.state = state;
        this.city = city;
    }

    public TextPair(String city, String state) {
        this.state = new Text(state);
        this.city = new Text(city);
    }

    public Text getState() {
        return state;
    }

    public void setState(Text state) {
        this.state = state;
    }

    public Text getCity() {
        return city;
    }

    public void setCity(Text city) {
        this.city = city;
    }


    @Override
    public String toString() {
        return "TextPair{" +
                "state=" + state +
                ", city=" + city +
                '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TextPair textPair = (TextPair) o;

        if (!state.equals(textPair.state)) return false;
        return city.equals(textPair.city);

    }

    @Override
    public int hashCode() {
        int result = state.hashCode();
        result = 31 * result + city.hashCode();
        return result;
    }

    //Do sorting based on State first, if states are equal compare the city
    public int compareTo(TextPair obj) {

        int result = state.compareTo(obj.state);
        if (result != 0) {
            return result;
        }
        return city.compareTo(obj.city);
    }

    // Serialize the Output
    public void write(DataOutput dataOutput) throws IOException {
        state.write(dataOutput);
        city.write(dataOutput);
    }
    // DeSerialize the Input
    public void readFields(DataInput dataInput) throws IOException {
        state.readFields(dataInput);
        city.readFields(dataInput);
    }
}
