package compiler.interfaces;

import java.io.Serializable;

public interface IConsumer<T> extends Serializable
{
    T next(T item);
}