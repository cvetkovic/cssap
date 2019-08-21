package compiler.graph;

import compiler.interfaces.basic.Source;
import compiler.interfaces.lambda.Function0;

public class FiniteSource<T> extends Source<T>
{
    private final Function0<T> code;
    private int numberOfItems;

    public FiniteSource(Function0 code, int numberOfItems)
    {
        this.code = code;
        this.numberOfItems = numberOfItems;
    }

    @Override
    public boolean hasNext()
    {
        return numberOfItems != 0;
    }

    @Override
    public T next()
    {
        if (numberOfItems == 0)
            throw new RuntimeException("The source has already produced number of tuples that were specified in its constructor.");

        numberOfItems--;
        return code.call();
    }
}
