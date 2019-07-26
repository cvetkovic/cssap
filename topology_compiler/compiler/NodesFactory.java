package compiler;

import compiler.interfaces.*;

import java.util.function.Predicate;

public class NodesFactory
{
    public static <T> IProducer<T> createSource(Function0<T> code)
    {
        return new IProducer<T>()
        {
            private IConsumer<T> consumer;

            @Override
            public T produce()
            {
                T generatedItem = code.call();
                /*if (generatedItem != null)
                    consumer.produce(generatedItem);*/

                return generatedItem;
            }

            @Override
            public void subscribe(IConsumer<T> consumer)
            {
                this.consumer = consumer;
            }
        };
    }

    public static<T,R> Operator<T> createMap(Function1<T,T> code)
    {
        return new Operator<T>()
        {
            private IConsumer<T> consumer;

            @Override
            public T next(T item)
            {
                T generatedItem = code.call(item);
                /*if (generatedItem != null)
                    consumer.produce(generatedItem);*/

                return generatedItem;
            }

            @Override
            public void subscribe(IConsumer<T> consumer)
            {
                this.consumer = consumer;
            }
        };
    }

    public static<T,R> Operator<T> createFilter(Predicate<T> predicate)
    {
        return new Operator<T>()
        {
            private IConsumer<T> consumer;

            @Override
            public T next(T item)
            {
                if (predicate.test(item))
                    return item;
                else
                    return null;
            }

            @Override
            public void subscribe(IConsumer<T> consumer)
            {
                this.consumer = consumer;
            }
        };
    }

    public static<T> Operator<T> createFold(T init, Function2<T,T,T> function)
    {
        return new Operator<T>()
        {
            private IConsumer<T> consumer;
            private T agg = init;

            @Override
            public T next(T item)
            {
                return function.call(agg, item);
            }

            @Override
            public void subscribe(IConsumer<T> consumer)
            {
                this.consumer = consumer;
            }
        };
    }

    public static<T> IConsumer<T> createSink()
    {
        return new IConsumer<T>()
        {
            @Override
            public T next(T item)
            {
                System.out.println(item);
                return null;
            }
        };
    }
}