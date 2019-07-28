package compiler.interfaces;

import java.io.Serializable;

public interface IProducer<T> extends Serializable
{
    void next();
    void subscribe(IConsumer<T> consumer);
    void setCallback(ICallback<T> callback);
}