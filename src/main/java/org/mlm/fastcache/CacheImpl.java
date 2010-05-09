package org.mlm.fastcache;

import org.mlm.fastcache.util.SpecialMap;

import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Date: May 8, 2010
 * Time: 2:57:06 PM
 */
public class CacheImpl<K,V> implements Cache<K,V>
{
    SpecialMap<K,Element<K,V>> map;
    LinkedList<Element<K,V>> expired = new LinkedList<Element<K,V>>();
    ScheduledExecutorService executor;

    public CacheImpl(int maxSize)
    {
        map = new SpecialMap<K, Element<K, V>>(maxSize)
        {
            @Override
            public void evict(SpecialMap.HashEntry<K, Element<K, V>> max)
            {
                // TODO evict
            }
        };
    }

    @Override
    public Element<K,V> get(K key)
    {
        Element<K,V> res;
        res = map.get(key);
        if (res != null)
        {
            if (res.getExpireTS() < System.currentTimeMillis())
            {
                synchronized (expired)
                {
                    expired.add(res);
                    if (expired.size() == 1)
                    {
                        executor.execute(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                synchronized (expired)
                                {
                                    for (Element<K,V> el : expired)
                                    {
                                        map.remove(el.getKey(), el);
                                    }
                                }

                            }
                        });
                    }
                }
                return null; // expired, just leave it
            }
        }
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void put(Element el)
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void evict(Object key)
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void flushAll()
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
