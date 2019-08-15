package compiler.structures;

import java.io.Serializable;

public class SuccessiveNumberGenerator implements Serializable
{
    private final int START_FROM = 1;

    private int number = START_FROM;

    public int next()
    {
        return number++;
    }
    public int getCurrentState() { return number; }
    public void increase() { number++; }
    public void reset() { number = START_FROM; }
}