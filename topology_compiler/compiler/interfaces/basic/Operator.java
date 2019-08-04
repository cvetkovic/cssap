package compiler.interfaces.basic;

import java.io.Serializable;

public abstract class Operator<A, B> implements Serializable, IConsumer<A>, IProducer<B>
{
    public enum Operation
    {
        COPY,
        ROUND_ROBIN_SPLITTER
    }

    protected int inputArity;
    private int parallelismHint = 1;
    protected IConsumer<B>[] consumers;
    // used to define what kind of grouping will be used for storm node linkage later
    protected Operation operation;
    protected String name;

    public Operator(String name, int inputArity, int outputArity, int parallelismHint)
    {
        this.name = name;
        this.inputArity = inputArity;
        this.consumers = new IConsumer[outputArity];
        this.parallelismHint = parallelismHint;
    }

    public Operator(String name, int inputArity, int outputArity, int parallelismHint, Operation operation)
    {
        this(name, inputArity, outputArity, parallelismHint);
        this.operation = operation;
    }

    public int getParallelismHint()
    {
        return parallelismHint;
    }

    public abstract void next(int channelIdentifier, A item);

    public IConsumer<B>[] getConsumers()
    {
        return consumers;
    }

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

    public Operation getOperation()
    {
        return operation;
    }

    public String getName()
    {
        return name;
    }
}