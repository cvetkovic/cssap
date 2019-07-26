package compiler.interfaces;

import java.io.Serializable;

public interface IActionSink<T> extends Serializable
{
    void init();
    void process(T item);
}