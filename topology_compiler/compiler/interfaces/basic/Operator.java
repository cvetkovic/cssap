package compiler.interfaces.basic;

import java.io.Serializable;

public abstract class Operator<A, B> implements Serializable, IConsumer<A>, IProducer<B>
{
    protected int inputArity;
    private int parallelismHint = 1;
    protected IConsumer<B>[] consumers;

    public Operator(int inputArity, int outputArity, int parallelismHint)
    {
        this.inputArity = inputArity;
        this.consumers = new IConsumer[outputArity];
        this.parallelismHint = parallelismHint;
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
}