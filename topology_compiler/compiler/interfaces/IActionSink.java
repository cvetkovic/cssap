package compiler.interfaces;

import java.io.Serializable;

public interface IActionSink<T> extends Serializable
{
    void process(T item);
}