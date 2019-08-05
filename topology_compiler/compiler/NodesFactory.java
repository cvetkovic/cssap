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

public class NodesFactory
{
    public static <A, B> AtomicOperator<A, B> map(String name, Function1<A, B> code)
    {
        return map(name, 1, code);
    }

    public static <A, B> AtomicOperator<A, B> map(String name, int parallelismHint, Function1<A, B> code)
    {
        return new AtomicOperator<A, B>(name, 1, 1, parallelismHint)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                consumers[0].next(channelIdentifier, code.call(item));
            }
        };
    }

    public static <A> AtomicOperator<A, A> filter(String name, SPredicate<A> predicate)
    {
        return filter(name, 1, predicate);
    }

    public static <A> AtomicOperator<A, A> filter(String name, int parallelismHint, SPredicate<A> predicate)
    {
        return new AtomicOperator<A, A>(name, 1, 1, parallelismHint)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                if (predicate.test(item))
                    consumers[0].next(channelIdentifier, item);
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
            throw new RuntimeException("Not applicable to parallel composition operator");

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
            throw new RuntimeException("Not applicable to parallel composition operator");

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
                operator1.subscribe(operator2);
                operator2.subscribe(consumers);

                if (getMostRightOperator(operator1).getOutputArity() != getMostLeftOperator(operator2).getInputArity())
                    throw new RuntimeException("Input arity of the first operator does not match the arity of the second operator.");
            }

            @Override
            public void next(int channelIdentifier, A item)
            {
                Operator mostLeft = getMostLeftOperator(operator1);
                mostLeft.next(channelIdentifier, item);
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
                mostLeft.next(channelIdentifier, item);
            }
        };
    }

    public static <A, B> AtomicOperator<A, B> fold(String name, B initial, Function2<B, A, B> function)
    {
        return new AtomicOperator<A, B>(name, 1, 1, 1)
        {
            private B accumulator = initial;

            @Override
            public void next(int channelIdentifier, A item)
            {
                accumulator = function.call(accumulator, item);
                consumers[0].next(channelIdentifier, accumulator);
            }
        };
    }

    public static <A> AtomicOperator<A, A> copy(String name, int outputArity)
    {
        return new AtomicOperator<A, A>(name, 1, outputArity, 1, Operator.Operation.COPY)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                for (int i = 0; i < consumers.length; i++)
                    consumers[i].next(i, item);
            }
        };
    }

    public static <A> AtomicOperator<A, A> merge(String name, int inputArity)
    {
        return new AtomicOperator<A, A>(name, inputArity, 1, 1)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                consumers[0].next(channelIdentifier, item);
            }
        };
    }

    public static <A> AtomicOperator<A, A> robinRoundSplitter(String name, int outputArity)
    {
        return new AtomicOperator<A, A>(name, 1, outputArity, 1, Operator.Operation.ROUND_ROBIN_SPLITTER)
        {
            private int sentTo = 0;

            @Override
            public void next(int channelIdentifier, A item)
            {
                consumers[sentTo].next(sentTo, item);
                sentTo = (sentTo + 1) % consumers.length;
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