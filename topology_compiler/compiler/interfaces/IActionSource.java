package compiler.interfaces;

import java.io.Serializable;

public interface IActionSource<T> extends Serializable
{
    void init();
    T process();
}