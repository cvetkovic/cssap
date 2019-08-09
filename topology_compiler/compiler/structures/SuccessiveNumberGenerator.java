package compiler.structures;

public class SuccessiveNumberGenerator
{
    private int number = 1;

    public int next()
    {
        return number++;
    }
    public int getCurrentState() { return number; }
    public void increase() { number++; }
}