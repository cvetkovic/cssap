public class KV<T, U>
{
    private T t;
    private U u;

    public KV(T t, U u)
    {
        this.t = t;
        this.u = u;
    }

    public T getT()
    {
        return t;
    }

    public void setT(T t)
    {
        this.t = t;
    }

    public U getU()
    {
        return u;
    }

    public void setU(U u)
    {
        this.u = u;
    }
}