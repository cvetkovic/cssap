package compiler.interfaces;

import java.io.Serializable;

public interface IActionOperator<T> extends Serializable
{
    void init();
    T process(T item);
}