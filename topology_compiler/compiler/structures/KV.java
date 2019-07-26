package compiler.structures;

import java.io.Serializable;

public class KV<K,V> implements Serializable
{
    private K k;
    private V v;

    public KV(K k, V v)
    {
        this.k = k;
        this.v = v;
    }

    public K getK()
    {
        return k;
    }

    public void setK(K k)
    {
        this.k = k;
    }

    public V getV()
    {
        return v;
    }

    public void setV(V v)
    {
        this.v = v;
    }

    @Override
    public String toString()
    {
        return Double.toString((Double)v);
    }
}