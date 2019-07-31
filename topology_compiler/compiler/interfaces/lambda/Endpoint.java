package compiler.interfaces.lambda;

import java.io.Serializable;

public interface Endpoint<T> extends Serializable
{
    void call(T item);
}