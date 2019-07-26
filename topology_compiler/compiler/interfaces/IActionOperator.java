package compiler.interfaces;

import java.io.Serializable;

public interface IActionOperator<T> extends Serializable
{
    T process(T item);
}