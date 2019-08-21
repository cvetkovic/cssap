package compiler.interfaces.basic;

import compiler.structures.KV;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class Operator<A, B> implements Serializable, IConsumer<A>, IProducer<B>
{
    protected int inputArity;
    private int parallelismHint = 1;
    protected IConsumer<B>[] consumers;
    protected Map<String, Integer> inputGates = new HashMap<>();
    protected Map<Integer, KV<String, String>> outputGates = new HashMap<>();
    protected String name;

    public Operator(String name, int inputArity, int outputArity, int parallelismHint)
    {
        this.name = name;
        this.inputArity = inputArity;
        this.consumers = new IConsumer[outputArity];
        this.parallelismHint = parallelismHint;
    }

    public int getParallelismHint()
    {
        return parallelismHint;
    }

    public abstract void next(int channelIdentifier, A item);

    @Override
    public int getInputArity()
    {
        return this.inputArity;
    }

    @Override
    public int getOutputArity()
    {
        return consumers.length;
    }

    @Override
    public void subscribe(IConsumer<B>... consumers)
    {
        if (consumers.length != this.consumers.length)
            throw new RuntimeException("You have to provide the same number of consumers as the number of output arity that was specified in the operator constructor.");

        this.consumers = consumers;
    }

    public String getName()
    {
        return name;
    }
    public IConsumer[] getConsumers()
    {
        return consumers;
    }
    public Operator[] getParallelConstituent()
    {
        return null;
    }
    public Map<String, Integer> getInputGates()
    {
        return inputGates;
    }
    public Map<Integer, KV<String, String>> getOutputGates() { return outputGates; }
}