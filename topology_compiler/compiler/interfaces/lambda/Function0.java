package compiler.interfaces.lambda;

import java.util.concurrent.Callable;

public interface Function0<R> extends Function, Callable<R>
{
    @Override
    R call();
}