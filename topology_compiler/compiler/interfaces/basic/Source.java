package compiler.interfaces.basic;

import compiler.structures.KV;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class Source<T> implements Iterator<T>, Serializable
{
    private Map<Integer, KV<String, String>> outputGates = new HashMap<>();

    public Map<Integer, KV<String, String>> getOutputGates()
    {
        return outputGates;
    }
}