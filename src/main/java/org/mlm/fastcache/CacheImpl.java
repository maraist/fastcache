package org.mlm.fastcache;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Date: May 8, 2010
 * Time: 2:57:06 PM
 */
public class CacheImpl<K,V> implements Cache<K,V>
{
    SpecialMap<K,Element<K,V>> map;
    Set<Element<K,V>> expired = new HashSet<Element<K,V>>();
    Set<Element<K,V>> readd = new HashSet<Element<K,V>>();
    ScheduledExecutorService executor;
    DiskStore diskStore;
    long liveTime;
    EvictionMode evictionMode;
    private boolean replicatePuts = false;
    private boolean replicatePutsViaEvict = false;
    private boolean replicateUpdates = false;
    private boolean replicateUpdatesViaEvict = false;
    private boolean replicateEvicts = false;

    public CacheImpl(int maxSize, EvictionMode em)
    {
        map = new SpecialMap<K, Element<K, V>>(maxSize, em)
        {
            @Override
            public void evict(SpecialMap.HashEntry<K, Element<K, V>> el)
            {
                if (diskStore != null)
                {
                    diskStore.enqueue(el.value);
                }
                super.evict(el);
            }
        };
    }

    @Override
    public Element<K,V> get(K key)
    {
        Element<K,V> el;
        el = map.get(key);
        if (el != null)
        {
            if (el.isExpired())
            {
                expireMessageOnGet(el);
                return null;
            }
            evictionMode.useElement(el);
            return el;
        }
        if (diskStore != null)
        {
            el = diskStore.get(key);
            if (el != null)
            {
                // we assume expiration is already tested
                evictionMode.useElement(el);
                readdMessageOnGet(el);
            }
        }
        return el;
    }

    private void readdMessageOnGet(Element<K, V> el)
    {
        synchronized (readd)
        {
            readd.add(el);
            if (readd.size() == 1)
            {
                executor.schedule(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        readdMessages();
                    }
                }, 100, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void expireMessageOnGet(Element<K, V> el)
    {
        synchronized (expired)
        {
            expired.add(el);
            if (expired.size() == 1)
            {
                executor.schedule(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        expireMessages();
                    }
                }, 100, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void readdMessages()
    {
        synchronized (readd)
        {
            long now = System.currentTimeMillis();
            for (Element<K,V> el : readd)
            {
                if (el.isExpired(now))
                {
                    // drop
                } else
                {
                    map.putIfAbsent(el.getKey(), el);
                }
            }
            readd.clear();
        }
    }

    private void expireMessages()
    {
        synchronized (expired)
        {
            for (Element<K,V> el : expired)
            {
                map.remove(el.getKey(), el); // if it's changed, ignore
            }
            expired.clear();
        }
    }

    @Override
    public void put(Element<K,V> el)
    {
        evictionMode.insertElement(el, liveTime);
        map.put(el.getKey(), el);
        if (replicatePuts)
        {
            if (replicatePutsViaEvict)
            {
                // TODO
            } else
            {
                // TODO
            }
        }
    }

    public void update(Element<K,V> el)
    {
        // retain previous statistics.
        evictionMode.insertElement(el, liveTime);
        map.put(el.getKey(), el);
        if (replicateUpdates)
        {
            if (replicateUpdatesViaEvict)
            {
                // TODO
            } else
            {
                // TODO
            }
        }
    }

    @Override
    public void evict(K key)
    {
        map.remove(key);
        if (diskStore != null)
        {
            diskStore.evict(key);
        }
        if (replicateEvicts)
        {
            // TODO
        }
    }

    @Override
    public void flushAll()
    {
        map.clear();
        if (diskStore != null)
        {
            diskStore.flushAll();
        }
        if (replicateEvicts)
        {
            // TODO
        }
    }

    public void onEvict(K key)
    {
        map.remove(key);
        if (diskStore != null)
        {
            diskStore.evict(key);
        }
    }
    public void onFlushAll()
    {
        map.clear();
        if (diskStore != null)
        {
            diskStore.flushAll();
        }
    }

    public void onPut(Element<K,V> el)
    {
        evictionMode.insertElement(el, liveTime);
        map.put(el.getKey(), el);
    }

    public void onUpdate(Element<K,V> el)
    {
        evictionMode.insertElement(el, liveTime);
        map.put(el.getKey(), el);
    }
}
