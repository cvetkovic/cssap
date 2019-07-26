package compiler.interfaces;

import java.io.Serializable;

public interface IProducer<T> extends Serializable
{
    void init();
    void next(T item);
    void subscribe(IConsumer<T> consumer);
}