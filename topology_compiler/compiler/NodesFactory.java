package compiler;

import compiler.interfaces.AtomicOperator;
import compiler.interfaces.ParallelComposition;
import compiler.interfaces.StreamComposition;
import compiler.interfaces.basic.IConsumer;
import compiler.interfaces.basic.Operator;
import compiler.interfaces.lambda.Endpoint;
import compiler.interfaces.lambda.Function1;
import compiler.interfaces.lambda.Function2;
import compiler.interfaces.lambda.SPredicate;

import java.util.Map;

public class NodesFactory
{
    public static <A, B> AtomicOperator<A, B> map(Function1<A, B> code)
    {
        return map(1, code);
    }

    public static <A, B> AtomicOperator<A, B> map(int parallelismHint, Function1<A, B> code)
    {
        return new AtomicOperator<A, B>(1, parallelismHint)
        {
            @Override
            public void subscribe(IConsumer<B>... consumers)
            {
                super.subscribe(consumers);

                if (mapOfConsumers.size() > 1)
                    throw new RuntimeException("Map operator can have only one input and one output.");
            }

            @Override
            public void next(int channelIdentifier, A item)
            {
                if (mapOfConsumers.containsKey(1))
                    mapOfConsumers.get(1).next(1, code.call(item));
                else
                    throw new RuntimeException("Map operator is not subscribed to any consumer.");
            }
        };
    }

    public static <A> AtomicOperator<A, A> filter(SPredicate<A> predicate)
    {
        return filter(1, predicate);
    }

    public static <A> AtomicOperator<A, A> filter(int parallelismHint, SPredicate<A> predicate)
    {
        return new AtomicOperator<A, A>(1, parallelismHint)
        {
            @Override
            public void subscribe(IConsumer<A>... consumers)
            {
                super.subscribe(consumers);

                if (mapOfConsumers.size() > 1)
                    throw new RuntimeException("Filter operator can have only one input and one output.");
            }

            @Override
            public void next(int channelIdentifier, A item)
            {
                if (mapOfConsumers.containsKey(1))
                {
                    if (predicate.test(item))
                        mapOfConsumers.get(1).next(1, item);
                }
                else
                    throw new RuntimeException("Channel with provided identifier is not subscribed to the operator.");
            }
        };
    }

    public static AtomicOperator getMostRightOperator(Operator operator)
    {
        if (operator instanceof AtomicOperator)
            return (AtomicOperator) operator;
        else if (operator instanceof StreamComposition)
        {
            Operator[] consistedOf = ((StreamComposition) operator).getConsistedOf();
            return getMostRightOperator(consistedOf[consistedOf.length - 1]);
        }
        else if (operator instanceof ParallelComposition)
            throw new RuntimeException("Not yet implemented.");

        return null;
    }

    public static AtomicOperator getMostLeftOperator(Operator operator)
    {
        if (operator instanceof AtomicOperator)
            return (AtomicOperator) operator;
        else if (operator instanceof StreamComposition)
        {
            Operator[] consistedOf = ((StreamComposition) operator).getConsistedOf();
            return getMostLeftOperator(consistedOf[0]);
        }
        else if (operator instanceof ParallelComposition)
            throw new RuntimeException("Not yet implemented.");

        return null;
    }

    // parallelism not allowed for composition
    public static <A, B, C> StreamComposition<A, C> streamComposition(Operator<A, B> operator1, Operator<B, C> operator2)
    {
        if (operator1 == operator2)
            throw new RuntimeException("Provided operators for stream composition must be distinct references.");

        return new StreamComposition<A, C>(new Operator[]{operator1, operator2})
        {
            @Override
            public void subscribe(IConsumer<C>... consumers)
            {
                // TODO: think about clearSubscription()
                //operator1.clearSubscription();

                operator1.subscribe(operator2);
                operator2.subscribe(consumers);

                if (getMostRightOperator(operator1).getOutputArity() != getMostLeftOperator(operator2).getInputArity())
                    throw new RuntimeException("Input arity of the first operator does not match the arity of the second operator.");
            }

            @Override
            public void next(int channelIdentifier, A item)
            {
                Operator mostLeft = getMostLeftOperator(operator1);

                if (mostLeft.getMapOfConsumers().containsKey(channelIdentifier))
                    mostLeft.next(channelIdentifier, item);
                else
                    throw new RuntimeException("Channel with provided identifier is not subscribed to the operator.");
            }
        };
    }

    // parallelism not allowed for composition
    public static <A, B, C, D> StreamComposition<A, D> streamComposition(Operator<A, B> operator1, Operator<B, C> operator2, Operator<C, D> operator3)
    {
        if (operator1 == operator2 || operator2 == operator3 || operator1 == operator3)
            throw new RuntimeException("Provided operators for stream composition must be distinct references.");

        return new StreamComposition<A, D>(new Operator[]{operator1, operator2, operator3})
        {
            @Override
            public void subscribe(IConsumer<D>... consumers)
            {
                // TODO: think about clearSubscription()
                //operator1.clearSubscription();
                //operator2.clearSubscription();

                operator1.subscribe(operator2);
                operator2.subscribe(operator3);
                operator3.subscribe(consumers);

                if (getMostRightOperator(operator1).getOutputArity() != getMostLeftOperator(operator2).getInputArity())
                    throw new RuntimeException("Input arity of the first operator does not match the arity of the second operator.");
                if (getMostRightOperator(operator2).getOutputArity() != getMostLeftOperator(operator3).getInputArity())
                    throw new RuntimeException("Input arity of the second operator does not match the arity of the third operator.");
            }

            @Override
            public void next(int channelIdentifier, A item)
            {
                Operator mostLeft = getMostLeftOperator(operator1);

                if (mostLeft.getMapOfConsumers().containsKey(channelIdentifier))
                    mostLeft.next(channelIdentifier, item);
                else
                    throw new RuntimeException("Channel with provided identifier is not subscribed to the operator.");
            }
        };
    }

    public static <A, B> AtomicOperator<A, B> fold(B initial, Function2<B, A, B> function)
    {
        return new AtomicOperator<A, B>(1, 1)
        {
            private B accumulator = initial;

            @Override
            public void subscribe(IConsumer<B>... consumers)
            {
                super.subscribe(consumers);

                if (mapOfConsumers.size() > 1)
                    throw new RuntimeException("Fold operator can have only one input and one output.");
            }

            @Override
            public void next(int channelIdentifier, A item)
            {
                if (mapOfConsumers.containsKey(1))
                {
                    accumulator = function.call(accumulator, item);
                    mapOfConsumers.get(1).next(1, accumulator);
                }
                else
                    throw new RuntimeException("Channel with provided identifier is not subscribed to the operator.");
            }
        };
    }

    public static <A> AtomicOperator<A, A> copy()
    {
        return new AtomicOperator<A, A>(1, 1)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                for (Map.Entry<Integer, IConsumer<A>> key : mapOfConsumers.entrySet())
                    key.getValue().next(key.getKey(), item);
            }
        };
    }

    public static <A> AtomicOperator<A, A> merge()
    {
        return new AtomicOperator<A, A>(1, 1)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                mapOfConsumers.get(1).next(channelIdentifier, item);
            }
        };
    }

    public static <A> AtomicOperator<A, A> robinRoundSplitter()
    {
        return new AtomicOperator<A, A>(1, 1)
        {
            private int lastSentTo = 0;
            private Integer[] channelNumbers;

            @Override
            public void subscribe(IConsumer<A>... consumers)
            {
                super.subscribe(consumers);
                channelNumbers = mapOfConsumers.keySet().toArray(new Integer[mapOfConsumers.size()]);
            }

            @Override
            public void next(int channelIdentifier, A item)
            {
                mapOfConsumers.get(channelNumbers[lastSentTo]).next(channelIdentifier, item);
                lastSentTo = (lastSentTo + 1) % channelNumbers.length;
            }
        };
    }

    public static <T> IConsumer<T> sink(Endpoint<T> code)
    {
        return new IConsumer<T>()
        {
            @Override
            public int getInputArity()
            {
                return 1;
            }

            @Override
            public void next(int channelNumber, T item)
            {
                code.call(item);
            }
        };
    }

    // parallelism not allowed for composition
    public static <T> ParallelComposition<T, T> parallelComposition(Operator<T, T> operator1, Operator<T, T> operator2)
    {
        if (operator1 == operator2)
            throw new RuntimeException("Provided operators for stream composition must be distinct references.");

        return new ParallelComposition<T, T>(new Operator[]{operator1, operator2})
        {
            @Override
            public void next(int channelIdentifier, T item)
            {
                int numberLeft = operator1.getInputArity();

                // channel numbering in both operators begin from one before merging
                if (channelIdentifier <= numberLeft)
                    operator1.next(channelIdentifier, item);
                else
                    operator2.next(channelIdentifier - numberLeft, item);
            }
        };
    }
}