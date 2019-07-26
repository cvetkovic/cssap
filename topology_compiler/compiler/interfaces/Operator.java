package compiler.interfaces;

import java.io.Serializable;

public abstract class Operator<T> implements Serializable, IConsumer<T>
{
    abstract public void subscribe(IConsumer<T> consumer);
}