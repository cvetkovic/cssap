package compiler.interfaces;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class Operator<A, B> implements Serializable, IConsumer<A>
{
    private int parallelismHint;
    private ICallback<B> callback;

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

    public void setCallback(ICallback<B> callback) { this.callback = callback; }
    public ICallback<B> getCallback() { return callback; }

    public void invokeCallback(B item) { callback.callback(item);}

    // reflection for pipeline merging purposes
    public static<A,B> Type getInputClassType(Operator<A,B> operator)
    {
        ParameterizedType type = (ParameterizedType)operator.getClass().getGenericSuperclass();
        return type.getActualTypeArguments()[0];
    }
    public static<A,B> Type getOutputClassType(Operator<A,B> operator)
    {
        ParameterizedType type = (ParameterizedType)operator.getClass().getGenericSuperclass();
        return type.getActualTypeArguments()[1];
    }
}