package compiler.interfaces.basic;

import java.io.Serializable;

public interface IConsumer<T> extends Serializable
{
    String getName();
    int getInputArity();
    void next(int channelNumber, T item);
}