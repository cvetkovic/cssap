package compiler.interfaces;

import java.io.Serializable;

public interface IProducer<T> extends Serializable
{
    T produce();
    void subscribe(IConsumer<T> consumer);
}