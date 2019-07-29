package compiler;

import compiler.interfaces.*;
import compiler.interfaces.lambda.Function0;
import compiler.interfaces.lambda.Function1;
import compiler.interfaces.lambda.Function2;
import compiler.interfaces.lambda.SPredicate;

public class NodesFactory
{
    public static <T> IProducer<T> createSource(Function0<T> code)
    {
        return new IProducer<T>()
        {
            private IConsumer<T> consumer;
            private ICallback<T> callbackInterface;

            @Override
            public void next()
            {
                callbackInterface.callback(code.call());
            }

            @Override
            public void setCallback(ICallback<T> callback)
            {
                this.callbackInterface = callback;
            }

            @Override
            public void subscribe(IConsumer<T> consumer)
            {
                this.consumer = consumer;
            }
        };
    }

    public static <A, B> Operator<A, B> createMap(Function1<A, B> code)
    {
        return createMap(1, code);
    }

    public static <A, B> Operator<A, B> createMap(int parallelismHint, Function1<A, B> code)
    {
        return new Operator<A, B>(parallelismHint)
        {
            private IConsumer<B> consumer;

            @Override
            public void subscribe(IConsumer<B> consumer)
            {
                this.consumer = consumer;
            }

            @Override
            public void next(A item)
            {
                invokeCallback(code.call(item));
            }
        };
    }

    public static <A> Operator<A, A> createFilter(SPredicate<A> predicate)
    {
        return createFilter(1, predicate);
    }

    public static <A> Operator<A, A> createFilter(int parallelismHint, SPredicate<A> predicate)
    {
        return new Operator<A, A>(parallelismHint)
        {
            private IConsumer<A> consumer;

            @Override
            public void subscribe(IConsumer<A> consumer)
            {
                this.consumer = consumer;
            }

            @Override
            public void next(A item)
            {
                if (predicate.test(item))
                    invokeCallback(item);
            }
        };
    }

    public static <A, B, C> Operator<A, C> composeOperator(Operator<A, B> operator1, Operator<B, C> operator2)
    {
        return composeOperator(1, operator1, operator2);
    }

    public static <A, B, C> Operator<A, C> composeOperator(int parallelismHint, Operator<A, B> operator1, Operator<B, C> operator2)
    {
        return new Operator<A, C>(parallelismHint)
        {
            @Override
            public void subscribe(IConsumer<C> consumer)
            {
                operator1.subscribe(operator2);
                operator2.subscribe(consumer);

                operator1.setCallback((item) -> operator2.next(item));
                operator2.setCallback((item) -> getCallback().callback(item));
            }

            @Override
            public void next(A item)
            {
                operator1.next(item);
            }
        };
    }

    public static <A, B, C, D> Operator<A, D> composeOperator(Operator<A, B> operator1, Operator<B, C> operator2, Operator<C, D> operator3)
    {
        return composeOperator(1, operator1, operator2, operator3);
    }

    public static <A, B, C, D> Operator<A, D> composeOperator(int parallelismHint, Operator<A, B> operator1, Operator<B, C> operator2, Operator<C, D> operator3)
    {
        return new Operator<A, D>(parallelismHint)
        {
            @Override
            public void subscribe(IConsumer<D> consumer)
            {
                operator1.subscribe(operator2);
                operator2.subscribe(operator3);
                operator3.subscribe(consumer);

                operator1.setCallback((item) -> operator2.next(item));
                operator2.setCallback((item) -> operator3.next(item));
                operator3.setCallback((item) -> getCallback().callback(item));
            }

            @Override
            public void next(A item)
            {
                operator1.next(item);
            }
        };
    }

    public static <A, B> Operator<A, B> createFold(B initial, Function2<B, A, B> function)
    {
        return new Operator<A, B>(1)
        {
            private IConsumer<B> consumer;
            private B accumulator = initial;

            @Override
            public void next(A item)
            {
                accumulator = function.call(accumulator, item);
                invokeCallback(accumulator);
            }

            @Override
            public void subscribe(IConsumer<B> consumer)
            {
                this.consumer = consumer;
            }
        };
    }

    public static <T> IConsumer<T> createSink()
    {
        return new IConsumer<T>()
        {
            @Override
            public void next(T item)
            {
                System.out.println(item);
            }
        };
    }
}