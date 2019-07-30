package compiler;

import compiler.interfaces.*;
import compiler.interfaces.lambda.Function0;
import compiler.interfaces.lambda.Function1;
import compiler.interfaces.lambda.Function2;
import compiler.interfaces.lambda.SPredicate;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class NodesFactory
{
    public static <T> IProducer<T> createSource()
    {
        return new IProducer<T>()
        {
            private List<IConsumer<T>> consumers = new LinkedList<>();

            @Override
            public void next(T item)
            {
                for (IConsumer<T> c : consumers)
                    c.next(item);
            }

            @Override
            public void next(IConsumer<T> rx, T item)
            {
                if (consumers.contains(rx))
                    rx.next(item);
                else
                    throw new RuntimeException("Operator was not subscribed to provided consumer.");
            }

            @Override
            public void subscribe(IConsumer<T> consumer)
            {
                if (!this.consumers.contains(consumer))
                    this.consumers.add(consumer);
            }

            @Override
            public void subscribe(List<IConsumer<T>> consumerList)
            {
                consumers.removeIf(p -> consumerList.contains(p));
                consumers.addAll(consumerList);
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
            @Override
            public void next(A item)
            {
                B result = code.call(item);
                for (IConsumer<B> c : consumers)
                    c.next(result);
            }

            @Override
            public void next(IConsumer<B> rx, A item)
            {
                if (consumers.contains(rx))
                {
                    rx.next(code.call(item));
                }
                else
                    throw new RuntimeException("Operator was not subscribed to provided consumer.");
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
            @Override
            public void next(A item)
            {
                if (predicate.test(item))
                    for (IConsumer<A> c : consumers)
                        c.next(item);
            }

            @Override
            public void next(IConsumer<A> rx, A item)
            {
                if (consumers.contains(rx))
                {
                    if (predicate.test(item))
                        rx.next(item);
                }
                else
                    throw new RuntimeException("Operator was not subscribed to provided consumer.");
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
            }

            @Override
            public void subscribe(List<IConsumer<C>> consumers)
            {
                operator1.subscribe(operator2);
                operator2.subscribe(consumers);
            }

            @Override
            public void next(A item)
            {
                operator1.next(item);
            }

            @Override
            public void next(IConsumer<C> rx, A item)
            {
                List<IConsumer<C>> ref = operator2.getConsumers();
                operator2.setConsumers(Collections.singletonList(rx));
                operator1.next(item);

                operator2.setConsumers(ref);
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
            }

            @Override
            public void subscribe(List<IConsumer<D>> consumers)
            {
                operator1.subscribe(operator2);
                operator2.subscribe(operator3);
                operator3.subscribe(consumers);
            }

            @Override
            public void next(A item)
            {
                operator1.next(item);
            }

            @Override
            public void next(IConsumer<D> rx, A item)
            {
                List<IConsumer<D>> ref = operator3.getConsumers();
                operator3.setConsumers(Collections.singletonList(rx));
                operator1.next(item);

                operator3.setConsumers(ref);
            }
        };
    }

    public static <A, B> Operator<A, B> createFold(B initial, Function2<B, A, B> function)
    {
        return new Operator<A, B>(1)
        {
            private B accumulator = initial;

            @Override
            public void next(A item)
            {
                accumulator = function.call(accumulator, item);
                for (IConsumer<B> c : consumers)
                    c.next(accumulator);
            }

            @Override
            public void next(IConsumer<B> rx, A item)
            {
                if (consumers.contains(rx))
                {
                    accumulator = function.call(accumulator, item);
                    for (IConsumer<B> c : consumers)
                        c.next(accumulator);
                }
                else
                    throw new RuntimeException("Operator was not subscribed to provided consumer.");
            }
        };
    }

    public static <T> IConsumer<T> createSink()
    {
        return (IConsumer<T>) item -> System.out.println(item);
    }
}