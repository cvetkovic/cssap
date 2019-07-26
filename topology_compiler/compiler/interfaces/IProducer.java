package compiler.interfaces;

import java.io.Serializable;

public interface IProducer<T> extends Serializable
{
    T next(T item);
    void subscribe(IConsumer<T> consumer);
}