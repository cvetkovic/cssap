package compiler.interfaces;

import java.io.Serializable;

public interface IProducer<T> extends Serializable
{
    void next(int channelNumber, T item);
    void subscribe(int channelNumber, IConsumer<T> consumer);
}