package compiler.interfaces;

import java.io.Serializable;

public interface IConsumer<T> extends Serializable
{
	int getInArity();
    void next(int channelNumber, T item);
}