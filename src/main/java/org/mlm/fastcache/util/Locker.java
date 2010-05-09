package org.mlm.fastcache.util;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Date: May 8, 2010
 * Time: 2:39:04 PM
 */
public class Locker
{
    private final Lock[] locks;
    private int mask;

    public Locker(int numLocks)
    {
        locks = new Lock[numLocks];
        for (int i = 0; i < numLocks; i++)
        {
            locks[i] = new ReentrantLock();
        }
        mask = numLocks;
    }

    private int hash(Object key)
    {
        return key.hashCode();
    }
    public <I, R> R execute(Object key, I i, LockerCallback<I,R> cb)
    {
        int idx = hash(key) & mask;
        Lock lock = locks[idx];
        lock.lock();
        try
        {
            return cb.execute(i);
        } finally
        {
            lock.unlock();
        }
    }
}
