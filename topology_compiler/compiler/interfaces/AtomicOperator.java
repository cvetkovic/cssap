package compiler.interfaces;

import compiler.interfaces.basic.Operator;

public abstract class AtomicOperator<A,B> extends Operator<A,B>
{
    public AtomicOperator(int inputArity, int parallelismHint)
    {
        super(inputArity, parallelismHint);
    }
}