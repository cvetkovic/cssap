package compiler.interfaces;

import java.io.Serializable;

public abstract class Operator<A, B> implements Serializable, IConsumer<A>
{
    private int parallelismHint;
    private ICallback<B> callback;

    public Operator(int parallelismHint)
    {
        this.parallelismHint = parallelismHint;
    }

    public abstract void next(A item);
    public abstract void subscribe(IConsumer<B> consumer);

    public void setCallback(ICallback<B> callback) { this.callback = callback; }
    public void invokeCallback(B item) { callback.callback(item);}

    // reflection for pipeline merging purposes
    A a;
    B b;

    public Class getInputClassType()
    {
        return a.getClass();
    }
    public Class getOutputClassType()
    {
        return b.getClass();
    }
    public int getParallelismHint()
    {
        return parallelismHint;
    }
}