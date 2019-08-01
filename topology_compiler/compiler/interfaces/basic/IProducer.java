package compiler.interfaces.basic;

import java.io.Serializable;

public interface IProducer<T> extends Serializable
{
    int getOutputArity();
    void subscribe(IConsumer<T>... consumer);
}