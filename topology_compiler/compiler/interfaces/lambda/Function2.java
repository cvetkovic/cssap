package compiler.interfaces.lambda;

public interface Function2<T1, T2, R> extends Function
{
    R call(T1 t1, T2 t2);
}
