package compiler.interfaces;

import java.io.Serializable;

public interface IConsumer<T> extends Serializable
{
    void init();
    void next(T item);
}