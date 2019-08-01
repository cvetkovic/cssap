package compiler.interfaces;

import compiler.interfaces.basic.Operator;

public abstract class ParallelComposition<A,B> extends Operator<A,B>
{
    private Operator[] consistedOf;

    public ParallelComposition(int inputArity, Operator[] consistedOf)
    {
        super(inputArity, 1);
        this.consistedOf = consistedOf;
    }

    public Operator[] getConsistedOf()
    {
        return consistedOf;
    }
}