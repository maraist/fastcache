package org.mlm.fastcache.util;

import org.mlm.fastcache.Cache;
import org.mlm.fastcache.Element;

/**
 * Date: May 8, 2010
 * Time: 2:45:07 PM
 */
public class AutoCreateCache<K,V> implements Cache<K,V>
{
    CacheObjectFactory<K,V> factory;
    Cache<K,V> cache;
    Locker locker = new Locker(1024);

    public AutoCreateCache(CacheObjectFactory<K,V> factory, Cache<K,V> cache)
    {
        this.factory = factory;
        this.cache = cache;
    }
    
    public Element<K,V> get(final K key)
    {
        Element<K,V> res = cache.get(key);
        if (res != null)
        {
            return res;
        }
        return locker.execute(key, key, new LockerCallback<K, Element<K,V>>() {
            @Override
            public Element<K,V> execute(K key)
            {
                Element<K,V> res = cache.get(key);
                if (res != null)
                {
                    return res;
                }
                 V val = factory.create(key);
                Element<K,V> el = new Element<K,V>(key, val, System.currentTimeMillis());
                cache.put(el);
                return el;
            }
        });
    }

    @Override
    public void put(final Element<K, V> el)
    {
        final K key = el.getKey();
        locker.execute(key, el, new LockerCallback<Element<K,V>, Object>()
        {
            @Override
            public Object execute(Element<K,V> el)
            {
                cache.put(el);
                return null;
            }
        });
    }



    @Override
    public void evict(final K key)
    {
        locker.execute(key, key, new LockerCallback<K, Object>()
        {
            @Override
            public Object execute(K key)
            {
                cache.evict(key);
                return null;
            }
        });
    }

    @Override
    public void flushAll()
    {
        cache.flushAll();
    }
}
