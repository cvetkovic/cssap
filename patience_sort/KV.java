package cvetkovic;

public class KV implements Comparable<KV>
{
    private double measurement;
    private int orderNumber;
    private long timestamp;

    public KV(double measurement, int orderNumber, long timestamp)
    {
        this.measurement = measurement;
        this.orderNumber = orderNumber;
        this.timestamp = timestamp;
    }

    public double getMeasurement()
    {
        return measurement;
    }

    public void setMeasurement(double measurement)
    {
        this.measurement = measurement;
    }

    public int getOrderNumber()
    {
        return orderNumber;
    }

    public void setOrderNumber(int orderNumber)
    {
        this.orderNumber = orderNumber;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(KV o)
    {
        return Long.valueOf(orderNumber).compareTo(Long.valueOf(o.orderNumber));
    }
}