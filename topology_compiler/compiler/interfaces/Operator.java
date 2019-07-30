package compiler.interfaces;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public abstract class Operator<A, B> implements Serializable, IConsumer<A>
{
    private int parallelismHint;
    protected List<IConsumer<B>> consumers = new LinkedList<>();

    public Operator(int parallelismHint)
    {
        this.parallelismHint = parallelismHint;
    }

    public int getParallelismHint()
    {
        return parallelismHint;
    }

    public abstract void next(A item);

    public abstract void next(IConsumer<B> rx, A item);

    public void subscribe(IConsumer<B> consumer)
    {
        if (!this.consumers.contains(consumer))
            this.consumers.add(consumer);
    }

    public void subscribe(List<IConsumer<B>> consumerList)
    {
        consumers.removeIf(p -> consumerList.contains(p));
        consumers.addAll(consumerList);
    }

    public List<IConsumer<B>> getConsumers()
    {
        return consumers;
    }

    public void setConsumers(List<IConsumer<B>> consumers)
    {
        this.consumers = consumers;
    }
}