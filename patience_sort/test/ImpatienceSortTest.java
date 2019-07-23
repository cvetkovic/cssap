package cvetkovic;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ImpatienceSortTest
{
    @Test
    public void PatienceSortTest() throws Exception
    {
        ImpatienceSortBuffer buffer = new ImpatienceSortBuffer();

        KV kv1 = new KV(1, 1, 1);
        KV kv2 = new KV(2, 2, 2);
        KV kv3 = new KV(3, 3, 3);
        KV kv4 = new KV(4, 4, 4);
        KV kv5 = new KV(5, 5, 5);
        KV kv6 = new KV(6, 6, 6);
        KV kv7 = new KV(7, 7, 7);
        KV kv8 = new KV(8, 8, 8);

        buffer.Insert(kv2);
        buffer.Insert(kv6);
        buffer.Insert(kv5);
        buffer.Insert(kv1);
        buffer.Insert(kv4);
        buffer.Insert(kv3);
        buffer.Insert(kv7);
        buffer.Insert(kv8);

        List<KV> list = buffer.GetAllKVLowerThan(9);
        int correntCount = 0;

        for (int i = 0; i < 8; i++)
        {
            if (list.get(i).getOrderNumber() == (i + 1))
                correntCount++;
        }

        Assert.assertEquals(8, correntCount);
    }*/
}