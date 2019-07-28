package compiler;

import compiler.interfaces.*;
import compiler.interfaces.lambda.Function0;
import compiler.interfaces.lambda.Function1;
import compiler.interfaces.lambda.Function2;

import java.util.function.Predicate;

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
                //consumer.next(code.call());

                // here callback has been implemented instead of the upper line
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

    public static <A> Operator<A, A> createFilter(Predicate<A> predicate)
    {
        return createFilter(1, predicate);
    }

    public static <A> Operator<A, A> createFilter(int parallelismHint, Predicate<A> predicate)
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
                operator2.subscribe(consumer);
                IConsumer<B> midpoint = new IConsumer<B>()
                {
                    @Override
                    public void next(B item)
                    {
                        operator2.next(item);
                    }
                };
                operator1.subscribe(midpoint);
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
                operator3.subscribe(consumer);
                IConsumer<C> thirdPoint = new IConsumer<C>()
                {
                    @Override
                    public void next(C item)
                    {
                        operator3.next(item);
                    }
                };
                IConsumer<B> secondPoint = new IConsumer<B>()
                {
                    @Override
                    public void next(B item)
                    {
                        operator2.next(item);
                    }
                };
                operator1.subscribe(secondPoint);
                operator2.subscribe(thirdPoint);
            }

            @Override
            public void next(A item)
            {
                operator1.next(item);
            }
        };
    }

    /*public static<T> Operator<T> createFold(T init, Function2<T,T,T> function)
    {
        return new Operator<T>()
        {
            private IConsumer<T> consumer;
            private T agg = init;

            @Override
            public void next(T item)
            {
                consumer.next(function.call(agg, item));
            }

            @Override
            public void subscribe(IConsumer<T> consumer)
            {
                this.consumer = consumer;
            }
        };
    }*/

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