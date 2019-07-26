package compiler.interfaces;

public interface Function1<T1,R> extends Function
{
    R call(T1 t1);
}