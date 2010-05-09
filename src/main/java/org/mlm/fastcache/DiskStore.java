package org.mlm.fastcache;

import java.io.*;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Date: May 8, 2010
 * Time: 3:53:16 PM
 */
public class DiskStore<K,V>
{
    final TreeMap<Integer,DiskRecord> freeMap = new TreeMap<Integer, DiskRecord>();
    final SpecialMap<K, Element<K,V>> smallMap;

    final SpecialMap<K, DiskRecord> diskMap;
    final ConcurrentHashMap<K,Element<K,V>> enqueueMap;

    final DiskRecord maxRecord = new DiskRecord(0, 0, Integer.MAX_VALUE, 0);
    private int maxRecords;
    private long maxLen;
    RandomAccessFile raf;
    private String cacheName;
    long endOfFilePos = 0;

    public void evict(K key)
    {
        //To change body of created methods use File | Settings | File Templates.
    }

    public void flushAll()
    {
        //To change body of created methods use File | Settings | File Templates.
    }

    class DiskRecord
    {
        long diskPos;
        int dataLen;
        int diskSpanLen;
        int refCnt;

        DiskRecord(long diskPos, int dataLen, int diskSpanLen, int refCnt)
        {
            this.diskPos = diskPos;
            this.dataLen = dataLen;
            this.diskSpanLen = diskSpanLen;
            this.refCnt = refCnt;
        }
    }

    ScheduledExecutorService executor;

    public DiskStore(String cacheName, ScheduledExecutorService executor, int maxRecords, long maxLen) throws FileNotFoundException
    {
        freeMap.put(Integer.MAX_VALUE, maxRecord);
        this.smallMap = new SpecialMap<K, Element<K, V>>(maxRecords);
        this.diskMap  = new SpecialMap<K, DiskRecord>(maxRecords)
        {
            @Override
            public void evict(SpecialMap.HashEntry<K, DiskRecord> el)
            {
                // TODO evict
                super.evict(el);
            }
        };
        this.enqueueMap = new SpecialMap<K, Element<K, V>>(maxRecords);
        this.cacheName = cacheName;
        raf = new RandomAccessFile(cacheName, "rw"); // TODO put in correct directory
        this.executor = executor;
        this.maxRecords = maxRecords;
        this.maxLen = maxLen;
    }

    public Element<K,V> get(K key)
    {
        synchronized (diskMap)
        {
            Element<K, V> el = null;
            synchronized (enqueueMap)
            {
                el = enqueueMap.get(key);
                if (el != null)
                {
                    enqueueMap.remove(el);
                    return el;
                }
            }
            el = smallMap.get(key);
            if (el != null)
            {
                return el;
            }
            DiskRecord dr = diskMap.get(key);
            if (dr == null)
            {
                return null;
            }
        }
    }

    public void enqueue(Element<K,V> el)
    {
        synchronized (enqueueMap)
        {
            enqueueMap.put(el, el);
            if (enqueueMap.size() == 1)
            {
                executor.schedule(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        processIn();
                    }
                }, 100, TimeUnit.MILLISECONDS); // allow a buildup
            }
        }
    }

    static class MyByteArrayOutputStream extends ByteArrayOutputStream
    {
        MyByteArrayOutputStream(int size)
        {
            super(size);
        }

        public byte[] getBuf()
        {
            return buf;
        }
    }

    /**
     *
     * @param o
     * @return -1 if not micro, else the effective in-mem size
     */
    private int isMicro(Object o)
    {
        Class clz = o.getClass();
        if (clz.isPrimitive() || clz.isEnum())
        {
            return 20;
        }
        if (Number.class.isAssignableFrom(clz)
            || Character.class.equals(clz)
            || Boolean.class.equals(clz))
        {
            return 20;
        }
        if (String.class.equals(clz))
        {
            String s = (String)o;
            int len = s.length();
            if (len < 128)
            {
                return 20 + len;
            }
        } else if (byte[].class.equals(clz))
        {
            byte[] b = (byte[])o;
            int len = b.length;
            if (len < 128)
            {
                return 12 + len;
            }
        }
        return -1;
    }

    private void processIn()

    {
        synchronized (this)
        {
            synchronized (diskMap)
            {
                synchronized (enqueueMap)
                {
                    if (enqueueMap.size() == 0)
                    {
                        return;
                    }
                    MyByteArrayOutputStream baos = null;
                    try
                    {
                        ObjectOutputStream oos = null;
                        LinkedList<Element<K,V>> ll = new LinkedList<Element<K,V>>();
                        for (Element<K,V> el : enqueueMap.values())
                        {
                            Object v = el.getValue();
                            int msz = isMicro(v);
                            if (msz > 0)
                            {
                                smallMap.put(el, el);
                            } else
                            {
                                if (oos == null)
                                {
                                    baos = new MyByteArrayOutputStream(16000);
                                    oos = new ObjectOutputStream(baos);
                                }
                                ll.add(el);
                                oos.writeObject(v);
                            }
                        }
                        if (oos != null)
                        {
                            int numEls = ll.size();
                            oos.close();
                            int sz = baos.size();
                            byte[] buff = baos.getBuf();
                            DiskRecord dr = freeMap.higherEntry(sz).getValue();
                            if (dr == maxRecord)
                            {
                                raf.seek(endOfFilePos);
                                raf.write(buff, 0, sz);
                                dr = new DiskRecord(endOfFilePos, sz, sz, numEls);
                            }
                            dr.refCnt = numEls;
                            dr.dataLen = sz;
                            for (Element<K,V> el : ll)
                            {
                                el.setValue(null);
                                diskMap.put(el, dr);
                            }
                        }

                    } catch (IOException e)
                    {
                        return;
                    }
                }
            }
        }
    }
}
