package compiler.interfaces.basic;

import java.util.HashMap;
import java.util.Map;

public abstract class Sink<T> implements IConsumer<T>
{
    private String name;
    private Map<String, Integer> inputGates = new HashMap<>();

    public Sink(String name)
    {
        this.name = name;
    }

    @Override
    public int getInputArity()
    {
        return 1;
    }

    @Override
    public abstract void next(int channelNumber, T item);

    public String getName()
    {
        return name;
    }
    public Map<String, Integer> getInputGates()
    {
        return inputGates;
    }
}