package compiler.interfaces;

import org.apache.storm.shade.com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
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

    /**
     * Broadcasts item to all the consumers that are subscribed to this operator
     * @param item An item to be sent
     */
    public abstract void next(A item);

    /**
     * Sends item to specific consumer that must be subscribed to this operator. Otherwise
     * RuntimeException will be thrown.
     * @param rx Reference to consumer that should receive the element
     * @param item An item to be sent
     */
    public abstract void next(IConsumer<B> rx, A item);

    /**
     * Subscribes provided consumer to the operators by adding it to the list of subscribed consumers.
     * Note that consumers that are already present in this operator and are tried to be added again
     * won't be duplicated.
     * @param consumer A consumer to subscribe to the operator
     */
    public void subscribe(IConsumer<B> consumer)
    {
        if (!this.consumers.contains(consumer))
            this.consumers.add(consumer);
    }

    /**
     * Subscribes provided consumers to the operators by adding them to the list of subscribed consumers.
     * Note that consumers that are already present in this operator and are tried to be added again
     * won't be duplicated.
     * @param consumerList A list of consumers to subscribe to the operator
     */
    public void subscribe(List<IConsumer<B>> consumerList)
    {
        consumerList.removeIf(p -> consumers.contains(p));
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