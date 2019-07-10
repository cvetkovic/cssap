package cssap;

public class KV
{
    private double measurement;
    private int orderNumber;

    public KV(double measurement, int orderNumber)
    {
        this.measurement = measurement;
        this.orderNumber = orderNumber;
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
}