package compiler;

import compiler.interfaces.IConsumer;
import compiler.interfaces.IProducer;
import compiler.interfaces.Operator;
import compiler.interfaces.lambda.Endpoint;
import compiler.interfaces.lambda.Function1;
import compiler.interfaces.lambda.Function2;
import compiler.interfaces.lambda.SPredicate;

import java.util.HashMap;
import java.util.Map;

public class NodesFactory
{
    public static <T> IProducer<T> createSource()
    {
        return new IProducer<T>()
        {
            private Map<Integer, IConsumer<T>> consumers = new HashMap<>();

            @Override
            public void next(int channelNumber, T item)
            {
                if (consumers.containsKey(channelNumber))
                    consumers.get(channelNumber).next(channelNumber, item);
                else
                    throw new RuntimeException("Channel with provided identifier is not subscribed to the operator.");
            }

            @Override
            public void subscribe(int channelNumber, IConsumer<T> consumer)
            {
                if (consumers.containsKey(channelNumber))
                    throw new RuntimeException("Channel with the same identifier already exists in the given operator.");

                consumers.put(channelNumber, consumer);
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
            public void subscribe(int channelNumber, IConsumer<B> consumer)
            {
                super.subscribe(1, consumer);

                if (consumers.size() > 1)
                    throw new RuntimeException("Fold operator can have only one input and one output.");
            }

            @Override
            public void next(int channelIdentifier, A item)
            {
                if (consumers.containsKey(1))
                    consumers.get(1).next(1, code.call(item));
                else
                    throw new RuntimeException("Map operator is not subscribed to any consumer.");
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
            public void subscribe(int channelNumber, IConsumer<A> consumer)
            {
                super.subscribe(1, consumer);

                if (consumers.size() > 1)
                    throw new RuntimeException("Fold operator can have only one input and one output.");
            }

            @Override
            public void next(int channelIdentifier, A item)
            {
                if (consumers.containsKey(1))
                {
                    if (predicate.test(item))
                        consumers.get(1).next(1, item);
                }
                else
                    throw new RuntimeException("Channel with provided identifier is not subscribed to the operator.");
            }
        };
    }

    public static <A, B, C> Operator<A, C> createStreamComposition(Operator<A, B> operator1, Operator<B, C> operator2)
    {
        return createStreamComposition(1, operator1, operator2);
    }

    public static <A, B, C> Operator<A, C> createStreamComposition(int parallelismHint, Operator<A, B> operator1, Operator<B, C> operator2)
    {
        if (operator1.getArity() > 1)
            throw new RuntimeException("Arity of first operator must be one.");

        return new Operator<A, C>(parallelismHint)
        {
            @Override
            public void subscribe(int channelNumber, IConsumer<C> consumer)
            {
                // ?what if some channel goes outside of composition?
                operator1.clearSubscription();

                operator1.subscribe(channelNumber, operator2);
                operator2.subscribe(channelNumber, consumer);
            }

            @Override
            public void next(int channelIdentifier, A item)
            {
                if (operator1.getConsumers().containsKey(channelIdentifier))
                    operator1.next(channelIdentifier, item);
                else
                    throw new RuntimeException("Channel with provided identifier is not subscribed to the operator.");
            }
        };
    }

    public static <A, B, C, D> Operator<A, D> createStreamComposition(Operator<A, B> operator1, Operator<B, C> operator2, Operator<C, D> operator3)
    {
        return createStreamComposition(1, operator1, operator2, operator3);
    }

    public static <A, B, C, D> Operator<A, D> createStreamComposition(int parallelismHint, Operator<A, B> operator1, Operator<B, C> operator2, Operator<C, D> operator3)
    {
        if (operator1.getArity() > 1)
            throw new RuntimeException("Arity of first operator must be one.");
        else if (operator2.getArity() > 2)
            throw new RuntimeException("Arity of second operator must be one.");

        return new Operator<A, D>(parallelismHint)
        {
            @Override
            public void subscribe(int channelNumber, IConsumer<D> consumer)
            {
                operator1.clearSubscription();
                operator2.clearSubscription();

                operator1.subscribe(channelNumber, operator2);
                operator2.subscribe(channelNumber, operator3);
                operator3.subscribe(channelNumber, consumer);
            }

            @Override
            public void next(int channelIdentifier, A item)
            {
                if (operator1.getConsumers().containsKey(channelIdentifier))
                    operator1.next(channelIdentifier, item);
                else
                    throw new RuntimeException("Channel with provided identifier is not subscribed to the operator.");
            }
        };
    }

    public static <A, B> Operator<A, B> createFold(B initial, Function2<B, A, B> function)
    {
        return new Operator<A, B>(1)
        {
            private B accumulator = initial;

            @Override
            public void subscribe(int channelNumber, IConsumer<B> consumer)
            {
                super.subscribe(1, consumer);

                if (consumers.size() > 1)
                    throw new RuntimeException("Fold operator can have only one input and one output.");
            }

            @Override
            public void next(int channelIdentifier, A item)
            {
                if (consumers.containsKey(1))
                {
                    accumulator = function.call(accumulator, item);
                    consumers.get(1).next(1, accumulator);
                }
                else
                    throw new RuntimeException("Channel with provided identifier is not subscribed to the operator.");
            }
        };
    }

    public static <A> Operator<A, A> createCopy()
    {
        return new Operator<A, A>(1)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                for (Map.Entry<Integer, IConsumer<A>> key : consumers.entrySet())
                    key.getValue().next(key.getKey(), item);
            }
        };
    }

    public static <A> Operator<A, A> createMerge()
    {
        return new Operator<A, A>(1)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                consumers.get(channelIdentifier).next(channelIdentifier, item);
            }
        };
    }

    public static <A> Operator<A, A> createSplitterRoundRobin()
    {
        return new Operator<A, A>(1)
        {
            private int lastSentTo = 0;
            private Integer[] channelNumbers;

            @Override
            public void subscribe(int channelNumber, IConsumer<A> consumer)
            {
                super.subscribe(channelNumber, consumer);
                channelNumbers = consumers.keySet().toArray(new Integer[consumers.size()]);
            }

            @Override
            public void next(int channelIdentifier, A item)
            {
                consumers.get(channelNumbers[lastSentTo]).next(channelIdentifier, item);
                lastSentTo = (lastSentTo + 1) % channelNumbers.length;
            }
        };
    }

    public static <T> IConsumer<T> createSink(Endpoint<T> code)
    {
        return (IConsumer<T>) (channelNumber, item) -> code.call(item);
    }

    public static <T> Operator<T,T> createParallelComposition(Operator<T,T> operator1, Operator<T,T> operator2)
    {
        return new Operator<T, T>(1)
        {
            @Override
            public void subscribe(int channelNumber, IConsumer<T> consumer)
            {
                super.subscribe(channelNumber, consumer);

                int id = 1;
                for (Map.Entry<Integer, IConsumer<T>> e : operator1.getConsumers().entrySet())
                    consumers.put(id++, e.getValue());
                for (Map.Entry<Integer, IConsumer<T>> e : operator2.getConsumers().entrySet())
                    consumers.put(id++, e.getValue());
            }

            @Override
            public void next(int channelIdentifier, T item)
            {
                int numberLeft = operator1.getArity();

                if (channelIdentifier <= numberLeft)
                    operator1.next(channelIdentifier, item);
                else
                    operator2.next(channelIdentifier - numberLeft, item);
            }
        };
    }
}