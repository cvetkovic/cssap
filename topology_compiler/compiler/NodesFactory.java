package compiler;

import compiler.interfaces.basic.Operator;
import compiler.interfaces.basic.Sink;
import compiler.interfaces.lambda.Endpoint;
import compiler.interfaces.lambda.Function1;
import compiler.interfaces.lambda.Function2;
import compiler.interfaces.lambda.SPredicate;

public class NodesFactory
{
    public static <A, B> Operator<A, B> map(String name, Function1<A, B> code)
    {
        return map(name, 1, code);
    }

    public static <A, B> Operator<A, B> map(String name, int parallelismHint, Function1<A, B> code)
    {
        return new Operator<A, B>(name, 1, 1, parallelismHint)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                consumers[0].next(channelIdentifier, code.call(item));
            }
        };
    }

    public static <A> Operator<A, A> filter(String name, SPredicate<A> predicate)
    {
        return filter(name, 1, predicate);
    }

    public static <A> Operator<A, A> filter(String name, int parallelismHint, SPredicate<A> predicate)
    {
        return new Operator<A, A>(name, 1, 1, parallelismHint)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                if (predicate.test(item))
                    consumers[0].next(channelIdentifier, item);
            }
        };
    }

    public static <A, B> Operator<A, B> fold(String name, B initial, Function2<B, A, B> function)
    {
        return new Operator<A, B>(name, 1, 1, 1)
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

    public static <A> Operator<A, A> copy(String name, int outputArity)
    {
        return new Operator<A, A>(name, 1, outputArity, 1)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                for (int i = 0; i < consumers.length; i++)
                    consumers[i].next(i, item);
            }
        };
    }

    public static <A> Operator<A, A> merge(String name, int inputArity)
    {
        return new Operator<A, A>(name, inputArity, 1, 1)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                consumers[0].next(channelIdentifier, item);
            }
        };
    }

    public static <A> Operator<A, A> robinRoundSplitter(String name, int outputArity)
    {
        return new Operator<A, A>(name, 1, outputArity, 1)
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

    public static <A> Sink<A> sink(String name, Endpoint<A> code)
    {
        return new Sink<A>(name)
        {
            @Override
            public void next(int channelNumber, A item)
            {
                code.call(item);
            }
        };
    }

    public static <A> Operator<A, A> duplicate(String name)
    {
        return new Operator<A, A>(name, 1, 1, 1)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                consumers[0].next(0, item);
                consumers[0].next(0, item);
            }
        };
    }

    public static <A> Operator<A,A> buffer(String name)
    {
        return new Operator<A, A>(name,1,1,1)
        {
            @Override
            public void next(int channelIdentifier, A item)
            {
                consumers[0].next(channelIdentifier, item);
            }
        };
    }
}