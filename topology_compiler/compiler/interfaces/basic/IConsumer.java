package compiler.interfaces.basic;

import java.io.Serializable;

public interface IConsumer<T> extends Serializable
{
    int getInputArity();
    void next(int channelNumber, T item);
}