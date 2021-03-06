package compiler.graph;

import compiler.interfaces.basic.Operator;
import compiler.interfaces.basic.Sink;
import compiler.interfaces.lambda.Endpoint;
import compiler.interfaces.lambda.Function1;
import compiler.interfaces.lambda.Function2;
import compiler.interfaces.lambda.SPredicate;
import compiler.storm.SystemMessage;
import compiler.structures.KV;

import java.util.Random;

public class NodesFactory
{
    public static <A, B> Operator<KV<A, SystemMessage>, KV<B, SystemMessage>> map(String name, Function1<A, B> code)
    {
        return map(name, 1, code);
    }

    public static <A, B> Operator<KV<A, SystemMessage>, KV<B, SystemMessage>> map(String name, int parallelismHint, Function1<A, B> code)
    {
        return new Operator<KV<A, SystemMessage>, KV<B, SystemMessage>>(name, 1, 1, parallelismHint)
        {
            @Override
            public void next(int channelIdentifier, KV<A, SystemMessage> item)
            {
                consumers[0].next(channelIdentifier, new KV(code.call(item.getK()), item.getV().clone()));
            }
        };
    }

    public static <A> Operator<KV<A, SystemMessage>, KV<A, SystemMessage>> filter(String name, SPredicate<A> predicate)
    {
        return filter(name, 1, predicate);
    }

    public static <A> Operator<KV<A, SystemMessage>, KV<A, SystemMessage>> filter(String name, int parallelismHint, SPredicate<A> predicate)
    {
        return new Operator<KV<A, SystemMessage>, KV<A, SystemMessage>>(name, 1, 1, parallelismHint)
        {
            @Override
            public void next(int channelIdentifier, KV<A, SystemMessage> item)
            {
                if (predicate.test(item.getK()))
                    consumers[0].next(channelIdentifier, new KV(item.getK(), item.getV().clone()));
            }
        };
    }

    public static <A, B> Operator<KV<A, SystemMessage>, KV<B, SystemMessage>> fold(String name, B initial, Function2<B, A, B> function)
    {
        return new Operator<KV<A, SystemMessage>, KV<B, SystemMessage>>(name, 1, 1, 1)
        {
            private B accumulator = initial;

            @Override
            public void next(int channelIdentifier, KV<A, SystemMessage> item)
            {
                accumulator = function.call(accumulator, item.getK());
                consumers[0].next(channelIdentifier, new KV(accumulator, item.getV().clone()));
            }
        };
    }

    public static <A> Operator<KV<A, SystemMessage>, KV<A, SystemMessage>> copy(String name, int outputArity)
    {
        return new Operator<KV<A, SystemMessage>, KV<A, SystemMessage>>(name, 1, outputArity, 1)
        {
            @Override
            public void next(int channelIdentifier, KV<A, SystemMessage> item)
            {
                for (int i = 0; i < consumers.length; i++)
                    consumers[i].next(i, new KV(item.getK(), item.getV().clone()));
            }
        };
    }

    public static <A> Operator<KV<A, SystemMessage>, KV<A, SystemMessage>> copyWithRandomSelectivity(String name, int outputArity)
    {
        return new Operator<KV<A, SystemMessage>, KV<A, SystemMessage>>(name, 1, outputArity, 1)
        {
            Random r = new Random();

            @Override
            public void next(int channelIdentifier, KV<A, SystemMessage> item)
            {
                for (int i = 0; i < consumers.length; i++)
                {
                    int bound = (int) (5 * r.nextDouble());
                    for (int j = 0; j < bound; j++)
                        consumers[i].next(i, new KV(item.getK(), item.getV().clone()));
                }
            }
        };
    }

    public static <A> Operator<KV<A, SystemMessage>, KV<A, SystemMessage>> merge(String name, int inputArity)
    {
        return new Operator<KV<A, SystemMessage>, KV<A, SystemMessage>>(name, inputArity, 1, 1)
        {
            @Override
            public void next(int channelIdentifier, KV<A, SystemMessage> item)
            {
                consumers[0].next(0, item);
            }
        };
    }

    /*public static <A> Operator<KV<A, SystemMessage>, KV<A, SystemMessage>> robinRoundSplitter(String name, int outputArity)
    {
        return new Operator<KV<A, SystemMessage>, KV<A, SystemMessage>>(name, 1, outputArity, 1)
        {
            private int sentTo = 0;

            @Override
            public void next(int channelIdentifier, KV<A, SystemMessage> item)
            {
                consumers[sentTo].next(sentTo, item);
                sentTo = (sentTo + 1) % consumers.length;
            }
        };
    }*/

    public static <A> Sink<KV<A, SystemMessage>> sink(String name, Endpoint<A> code)
    {
        return new Sink<KV<A, SystemMessage>>(name)
        {
            @Override
            public void next(int channelNumber, KV<A, SystemMessage> item)
            {
                if (item.getK() == null)
                    code.call((A)"END_OF_STREAM");
                else
                    code.call(item.getK());

                /*else
                {
                    System.out.print(item.getV().getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER).toString());
                    System.out.print(" - ");*/

                //}
            }
        };
    }
}