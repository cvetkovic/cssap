package compiler.interfaces;

import java.io.Serializable;

public abstract class Operator<A, B> implements Serializable, IConsumer<A>
{
    private int parallelismHint;

    public Operator(int parallelismHint)
    {
        this.parallelismHint = parallelismHint;
    }
    public int getParallelismHint()
    {
        return parallelismHint;
    }

    public abstract void next(A item);
    public abstract void subscribe(IConsumer<B> consumer);
}