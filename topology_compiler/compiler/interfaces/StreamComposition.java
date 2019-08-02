package compiler.interfaces;

import compiler.NodesFactory;
import compiler.interfaces.basic.Operator;

public abstract class StreamComposition<A,B> extends Operator<A,B>
{
    private Operator[] consistedOf;

    public StreamComposition(Operator[] consistedOf)
    {
        super(1);
        this.consistedOf = consistedOf;
        this.inputArity = NodesFactory.getMostLeftOperator(consistedOf[0]).getInputArity();
    }

    public Operator[] getConsistedOf()
    {
        return consistedOf;
    }
}