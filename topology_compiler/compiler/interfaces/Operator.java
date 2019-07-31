package compiler.interfaces;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class Operator<A, B> implements Serializable, IConsumer<A>, IProducer<A>
{
    private int parallelismHint;
    protected Map<Integer, IConsumer<B>> consumers = new HashMap<>();

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
    /*public void next(A item)
    {
        for (Integer key : consumers.keySet())
            next(key, item);
    }*/

    /**
     * Sends item to specific consumer that must be subscribed to this operator. Otherwise
     * RuntimeException will be thrown.
     * @param channelIdentifier Identifier of a channel through which item will be sent
     * @param item An item to be sent
     */
    public abstract void next(int channelIdentifier, A item);

    /**
     * Subscribes provided consumer to the operator and marks it with provided channel number.
     * If the channel has already been taken an exception will be thrown.
     * @param channelNumber Channel identifier
     * @param consumer Reference to a consumer
     */
    public void subscribe(int channelNumber, IConsumer<B> consumer)
    {
        if (consumers.containsKey(channelNumber))
            throw new RuntimeException("Channel with the same identifier already exists in the given operator.");

        consumers.put(channelNumber, consumer);
    }

    public void clearSubscription()
    {
        consumers.clear();
    }

    public Map<Integer, IConsumer<B>> getConsumers()
    {
        return consumers;
    }

    public int getArity()
    {
        return consumers.size();
    }
}