package compiler.interfaces.basic;

public abstract class Sink<T> implements IConsumer<T>
{
    private String name;

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
}