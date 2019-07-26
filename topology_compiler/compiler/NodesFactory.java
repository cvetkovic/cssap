package compiler;

import compiler.interfaces.*;

public class NodesFactory
{
    public static <T> IProducer<T> createSource(IActionSource<T> code)
    {
        return new IProducer<T>()
        {
            private IConsumer<T> consumer;

            @Override
            public void init()
            {
                code.init();
            }

            @Override
            public void next(T item)
            {
                T generatedItem = code.process();
                if (generatedItem != null)
                    consumer.next(generatedItem);
            }

            @Override
            public void subscribe(IConsumer<T> consumer)
            {
                this.consumer = consumer;
            }
        };
    }

    public static <T> Operator<T> createOperator(IActionOperator<T> code)
    {
        return new Operator<T>()
        {
            private IConsumer<T> consumer;

            @Override
            public void init()
            {
                code.init();
            }

            @Override
            public void next(T item)
            {
                T operationDone = code.process(item);
                if (operationDone != null)
                    consumer.next(operationDone);
            }

            @Override
            public void subscribe(IConsumer<T> consumer)
            {
                this.consumer = consumer;
            }
        };
    }

    public static <T> IConsumer<T> createSink(IActionSink<T> code)
    {
        return new IConsumer<T>()
        {
            @Override
            public void init()
            {
                code.init();
            }

            @Override
            public void next(T item)
            {
                if (item != null)
                    code.process(item);
            }
        };
    }
}