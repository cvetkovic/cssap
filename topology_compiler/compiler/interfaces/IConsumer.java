package compiler.interfaces;

import java.io.Serializable;

public interface IConsumer<T> extends Serializable
{
    void next(T item);
}