package compiler.interfaces;

import java.io.Serializable;
import java.util.List;

public interface IProducer<T> extends Serializable
{
    void next();
    void subscribe(IConsumer<T> consumer);
    void subscribe(List<IConsumer<T>> consumer);
}