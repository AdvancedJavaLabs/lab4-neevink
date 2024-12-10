package org.example;

import org.apache.hadoop.io.Writable;

import java.io.IOException;

public class SalesData implements Writable {
    private double revenue;
    private int quantity;

    public SalesData() {
    }

    public SalesData(double revenue, int quantity) {
        this.revenue = revenue;
        this.quantity = quantity;
    }

    @Override
    public void write(java.io.DataOutput out) throws IOException {
        out.writeDouble(revenue);
        out.writeInt(quantity);
    }

    @Override
    public void readFields(java.io.DataInput in) throws IOException {
        revenue = in.readDouble();
        quantity = in.readInt();
    }

    public double getRevenue() {
        return revenue;
    }

    public int getQuantity() {
        return quantity;
    }

    public void add(SalesData other) {
        this.revenue += other.revenue;
        this.quantity += other.quantity;
    }
}