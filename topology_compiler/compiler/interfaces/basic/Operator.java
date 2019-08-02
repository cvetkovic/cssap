package compiler.interfaces.basic;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class Operator<A, B> implements Serializable, IConsumer<A>, IProducer<B>
{
    protected int inputArity;
    private int parallelismHint = 1;
    private int newChannel = 1;
    protected Map<Integer, IConsumer<B>> mapOfConsumers = new HashMap<>();

    public Operator(int parallelismHint)
    {
        this.parallelismHint = parallelismHint;
    }

    public Operator(int inputArity, int parallelismHint)
    {
        this.inputArity = inputArity;
        this.parallelismHint = parallelismHint;
    }

    public int getParallelismHint()
    {
        return parallelismHint;
    }

    public abstract void next(int channelIdentifier, A item);

    public void clearSubscription()
    {
        mapOfConsumers.clear();
        newChannel = 1;
    }

    public Map<Integer, IConsumer<B>> getMapOfConsumers()
    {
        return mapOfConsumers;
    }

    @Override
    public int getInputArity()
    {
        return this.inputArity;
    }

    @Override
    public int getOutputArity()
    {
        return mapOfConsumers.size();
    }

    @Override
    public void subscribe(IConsumer<B>... consumers)
    {
        for (int i = 0; i < consumers.length; i++)
            mapOfConsumers.put(newChannel++, consumers[i]);
    }
}