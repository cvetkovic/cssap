package compiler;

import compiler.interfaces.basic.Operator;
import compiler.interfaces.lambda.Endpoint;
import compiler.interfaces.lambda.Function1;
import compiler.interfaces.lambda.Function2;
import compiler.interfaces.lambda.SPredicate;

public class NodesFactory
{
    public static <A, B> AtomicGraph map(String name, Function1<A, B> code)
    {
        return map(name, 1, code);
    }

    public static <A, B> AtomicGraph map(String name, int parallelismHint, Function1<A, B> code)
    {
        return new AtomicGraph(new Operator<A, B>(name, 1, 1, parallelismHint)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                consumers[0].next(channelIdentifier, code.call(item));
            }
        });
    }

    public static <A> AtomicGraph filter(String name, SPredicate<A> predicate)
    {
        return filter(name, 1, predicate);
    }

    public static <A> AtomicGraph filter(String name, int parallelismHint, SPredicate<A> predicate)
    {
        return new AtomicGraph(new Operator<A, A>(name, 1, 1, parallelismHint)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                if (predicate.test(item))
                    consumers[0].next(channelIdentifier, item);
            }
        });
    }

    public static <A, B> AtomicGraph fold(String name, B initial, Function2<B, A, B> function)
    {
        return new AtomicGraph(new Operator<A, B>(name, 1, 1, 1)
        {
            private B accumulator = initial;

            @Override
            public void next(int channelIdentifier, A item)
            {
                accumulator = function.call(accumulator, item);
                consumers[0].next(channelIdentifier, accumulator);
            }
        });
    }

    public static <A> AtomicGraph copy(String name, int outputArity)
    {
        return new AtomicGraph(new Operator<A, A>(name, 1, outputArity, 1, Operator.Operation.COPY)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                for (int i = 0; i < consumers.length; i++)
                    consumers[i].next(i, item);
            }
        });
    }

    public static <A> AtomicGraph merge(String name, int inputArity)
    {
        return new AtomicGraph(new Operator<A, A>(name, inputArity, 1, 1)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                consumers[0].next(channelIdentifier, item);
            }
        });
    }

    public static <A> AtomicGraph robinRoundSplitter(String name, int outputArity)
    {
        return new AtomicGraph(new Operator<A, A>(name, 1, outputArity, 1, Operator.Operation.ROUND_ROBIN_SPLITTER)
        {
            private int sentTo = 0;

            @Override
            public void next(int channelIdentifier, A item)
            {
                consumers[sentTo].next(sentTo, item);
                sentTo = (sentTo + 1) % consumers.length;
            }
        });
    }

    public static <A> AtomicGraph sink(String name, Endpoint<A> code)
    {
        return new AtomicGraph(new Operator<A, A>(name, 1, 0, 1)
        {
            private int sentTo = 0;

            @Override
            public void next(int channelIdentifier, A item)
            {
                code.call(item);
            }
        });
    }
}