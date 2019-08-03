package compiler.interfaces;

import compiler.NodesFactory;
import compiler.interfaces.basic.Operator;

public abstract class StreamComposition<A, B> extends Operator<A, B>
{
    private Operator[] consistedOf;

    public StreamComposition(Operator[] consistedOf)
    {
        super(NodesFactory.getMostLeftOperator(consistedOf[0]).getInputArity(),
                NodesFactory.getMostRightOperator(consistedOf[consistedOf.length - 1]).getOutputArity(),
                1);

        this.consistedOf = consistedOf;
    }

    public Operator[] getConsistedOf()
    {
        return consistedOf;
    }
}