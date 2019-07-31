package compiler.interfaces;

import java.io.Serializable;

public interface IProducer<T> extends Serializable
{
    int getOutArity();
    void subscribe(IConsumer<T> consumer...);
}