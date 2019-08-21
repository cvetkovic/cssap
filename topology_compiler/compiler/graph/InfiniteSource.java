package compiler.graph;

import compiler.interfaces.basic.Source;
import compiler.interfaces.lambda.Function0;

public class InfiniteSource<T> extends Source<T>
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
