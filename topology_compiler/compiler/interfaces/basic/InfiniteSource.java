package compiler.interfaces.basic;

import compiler.interfaces.lambda.Function0;

import java.util.Iterator;

public class InfiniteSource<T> implements Iterator<T>
{
    private final Function0<T> code;

    public InfiniteSource(Function0 code)
    {
        this.code = code;
    }

    @Override
    public boolean hasNext()
    {
        return true;
    }

    @Override
    public T next()
    {
        return code.call();
    }
}
