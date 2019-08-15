package compiler.interfaces.basic;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class Source<T> implements Iterator<T>, Serializable
{
    private Map<Integer, String> outputGates = new HashMap<>();

    public Map<Integer, String> getOutputGates()
    {
        return outputGates;
    }
}