package org.maraist.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * User: maraist
 * Date: Aug 19, 2010
 * Time: 2:11:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class Cache
{
    @SuppressWarnings({"UnusedDeclaration"})
    final static Logger LOG = LoggerFactory.getLogger(Cache.class);


    static enum KType
    {
        deleted, b, c, s, i, l, f, d, s8, str, obj
    }

    KType[] ktypes = KType.values();

    private long startupts = System.currentTimeMillis();

    int maxSize;
    int maxExp;
    int maxIdle;

    public void setMaxSize(int maxSize)
    {
        this.maxSize = maxSize;
    }

    public void setMaxExp(int maxExp)
    {
        this.maxExp = maxExp;
    }

    public void setMaxIdle(int maxIdle)
    {
        this.maxIdle = maxIdle;
    }

    public Object get(Object key)
    {
        if (key instanceof Number)
        {
            Number n = (Number)key;
            if (key instanceof Integer)
            {
                return getInt(n.intValue());
            } else if (key instanceof Long)
            {
                return getLong(n.longValue());
            } else if (key instanceof Float)
            {
                return getFloat(n.floatValue());
            } else if (key instanceof Double)
            {
                return getDouble(n.doubleValue());
            } else if (key instanceof Short)
            {
                return getShort(n.shortValue());
            } else if (key instanceof Byte)
            {
                return getByte(n.byteValue());
            }
            // def
            return getLong(n.longValue());
        } else if (key instanceof Character)
        {
            return getChar((Character)key);
        } else if (key instanceof String)
        {
            String skey = (String)key;
            return getString(skey);
        }
        return getObj(key, KType.obj);
    }

    public void put(Object key, Object val)
    {
        if (key instanceof Number)
        {
            Number n = (Number)key;
            if (key instanceof Integer)
            {
                putInt(n.intValue(), val);
            } else if (key instanceof Long)
            {
                putLong(n.longValue(), val);
            } else if (key instanceof Float)
            {
                putFloat(n.floatValue(), val);
            } else if (key instanceof Double)
            {
                putDouble(n.doubleValue(), val);
            } else if (key instanceof Short)
            {
                putShort(n.shortValue(), val);
            } else if (key instanceof Byte)
            {
                putByte(n.byteValue(), val);
            }
            // def
            putLong(n.longValue(), val);
        } else if (key instanceof Character)
        {
            putChar((Character)key, val);
        } else if (key instanceof String)
        {
            String skey = (String)key;
            putString(skey, val);
        }
        putObj(key, val, KType.obj);
    }


    public Object getString(String skey)
    {
        if (skey.length() <= 8)
        {
            if (skey.length() == 0)
            {
                return getI(0, KType.s8);
            }
            byte[] kbytes = skey.getBytes();
            if (kbytes.length <= 4)
            {
                int l = kbytes[0];
                for (int i = 1; i < kbytes.length; i++)
                {
                    l <<= 8;
                    l |= kbytes[i];
                }
                return getI(l, KType.s8);
            } else if (kbytes.length <= 8)
            {
                long l = kbytes[0];
                for (int i = 1; i < kbytes.length; i++)
                {
                    l <<= 8;
                    l |= kbytes[i];
                }
                return getL(l, KType.s8);
            }
        }
        return getObj(skey, KType.obj);
    }

    public void putString(String skey, Object val)
    {
        if (skey.length() <= 8)
        {
            if (skey.length() == 0)
            {
                putI(0, val, KType.s8);
            }
            byte[] kbytes = skey.getBytes();
            if (kbytes.length <= 4)
            {
                int l = kbytes[0];
                for (int i = 1; i < kbytes.length; i++)
                {
                    l <<= 8;
                    l |= kbytes[i];
                }
                putI(l, val, KType.s8);
            } else if (kbytes.length <= 8)
            {
                long l = kbytes[0];
                for (int i = 1; i < kbytes.length; i++)
                {
                    l <<= 8;
                    l |= kbytes[i];
                }
                putL(l, val, KType.s8);
            }
        }
        putObj(skey, val, KType.obj);
    }


    public Object getByte(byte key)
    {
        return getI(key, KType.b);
    }

    public Object getChar(char key)
    {
        return getI(key, KType.c);
    }

    public Object getShort(short key)
    {
        return getI(key, KType.s);
    }

    public Object getFloat(float key)
    {
        int i = Float.floatToRawIntBits(key);
        return getI(i, KType.f);
    }

    public Object getDouble(double key)
    {
        long i = Double.doubleToRawLongBits(key);
        return getL(i, KType.d);
    }

    public Object getInt(int key)
    {
        return getI(key, KType.i);
    }

    public Object getLong(long key)
    {
        return getL(key, KType.l);
    }

    public void putByte(byte key, Object val)
    {
        putI(key, val, KType.b);
    }

    public void putChar(char key, Object val)
    {
        putI(key, val, KType.c);
    }

    public void putShort(short key, Object val)
    {
        putI(key, val, KType.s);
    }

    public void putFloat(float key, Object val)
    {
        int i = Float.floatToRawIntBits(key);
        putI(i, val, KType.f);
    }

    public void putDouble(double key, Object val)
    {
        long i = Double.doubleToRawLongBits(key);
        putL(i, val, KType.d);
    }

    public void putInt(int key, Object val)
    {
        putI(key, val, KType.i);
    }

    public void putLong(long key, Object val)
    {
        putL(key, val, KType.l);
    }

    private Object getI(int key, KType t)
    {
        if (itable == null)
        {
            return null;
        }
        int tidx = tablehashidx(key);
        IRecord r = itable[tidx];
        if (r == null)
        {
            return null;
        }

        return getIRecurse(key, t, r, r.next);
    }

    private Object getIRecurse(int key, KType kt, IRecord r, int nxt)
    {
        int code = nextCode(nxt);
        int nxtAddr = nextAddr(nxt);
        int[] data;
        switch (code)
        {
            case 0: // index node
            {
                data = r.idata;
                if (data[nxtAddr + 3] <= key)
                {
                    if (data[nxtAddr + 1] <= key)
                    {
                        if (data[nxtAddr] <= key)
                        {
                            return getIRecurse(key, kt, r, data[nxtAddr + 7]);
                        } else
                        {
                            return getIRecurse(key, kt, r, data[nxtAddr + 8]);
                        }
                    } else
                    {
                        if (data[nxtAddr + 2] <= key)
                        {
                            return getIRecurse(key, kt, r, data[nxtAddr + 9]);
                        } else
                        {
                            return getIRecurse(key, kt, r, data[nxtAddr + 10]);
                        }
                    }
                } else
                {
                    if (data[nxtAddr + 5] <= key)
                    {
                        if (data[nxtAddr + 4] <= key)
                        {
                            return getIRecurse(key, kt, r, data[nxtAddr + 11]);
                        } else
                        {
                            return getIRecurse(key, kt, r, data[nxtAddr + 12]);
                        }
                    } else
                    {
                        if (data[nxtAddr + 6] <= key)
                        {
                            return getIRecurse(key, kt, r, data[nxtAddr + 13]);
                        } else
                        {
                            return getIRecurse(key, kt, r, data[nxtAddr + 14]);
                        }
                    }
                }
            }
            case 1: // single-leaf
            {
                data = r.d1;
                if (data[nxtAddr] == key)
                {
                    int flags = data[nxtAddr + 1];
                    int val = data[nxtAddr + 2];

                    if (kt == flagKType(flags))
                    {
                        long now = 0;
                        if (maxExp > 0)
                        {
                            long expTs = code2Time(data[nxtAddr + 3]);
                            now = System.currentTimeMillis();
                            if (expTs < now)
                            {
                                return null;
                            }
                        }
                        // not sure if this is necessary
                        if (maxIdle > 0)
                        {
                            long idleExpTs = code2Time(data[nxtAddr + 4]);
                            if (now == 0)
                            {
                                now = System.currentTimeMillis();
                            }
                            if (idleExpTs < now)
                            {
                                return null;
                            }
                        }
                        Object res = returnObjectI(flags, val);
                        if (res != null && maxIdle > 0)
                        {
                            data[nxtAddr + 4] = time2Code(now + maxIdle);
                        }
                        return res;
                    }
                }
                return null;
            }
            case 2: // 3-way leaf
            {
                data = r.d3;
                for (int i = 0; i < 3; i++, nxtAddr += 5)
                {
                    if (data[nxtAddr] == key)
                    {
                        // TODO text exp
                        int flags = data[nxtAddr + 1];
                        int val = data[nxtAddr + 2];

                        if (kt == flagKType(flags))
                        {
                            long now = 0;
                            if (maxExp > 0)
                            {
                                long expTs = code2Time(data[nxtAddr + 3]);
                                now = System.currentTimeMillis();
                                if (expTs < now)
                                {
                                    return null;
                                }
                            }
                            // not sure if this is necessary
                            if (maxIdle > 0)
                            {
                                long idleExpTs = code2Time(data[nxtAddr + 4]);
                                if (now == 0)
                                {
                                    now = System.currentTimeMillis();
                                }
                                if (idleExpTs < now)
                                {
                                    return null;
                                }
                            }
                            Object res = returnObjectI(flags, val);
                            if (res != null && maxIdle > 0)
                            {
                                data[nxtAddr + 4] = time2Code(now + maxIdle);
                            }
                            return res;
                        }
                    }
                }
                return null;
            }
            case 3: // 5-way leaf
            {
                data = r.d5;
                for (int i = 0; i < 5; i++, nxtAddr += 5)
                {
                    if (data[nxtAddr] == key)
                    {
                        // TODO text exp
                        int flags = data[nxtAddr + 1];
                        int val = data[nxtAddr + 2];

                        if (kt == flagKType(flags))
                        {
                            long now = 0;
                            if (maxExp > 0)
                            {
                                long expTs = code2Time(data[nxtAddr + 3]);
                                now = System.currentTimeMillis();
                                if (expTs < now)
                                {
                                    return null;
                                }
                            }
                            // not sure if this is necessary
                            if (maxIdle > 0)
                            {
                                long idleExpTs = code2Time(data[nxtAddr + 4]);
                                if (now == 0)
                                {
                                    now = System.currentTimeMillis();
                                }
                                if (idleExpTs < now)
                                {
                                    return null;
                                }
                            }
                            Object res = returnObjectI(flags, val);
                            if (res != null && maxIdle > 0)
                            {
                                data[nxtAddr + 4] = time2Code(now + maxIdle);
                            }
                            return res;
                        }
                    }
                }
                return null;
            }
            default:
                throw new RuntimeException("Invalid next-type");
        }
    }

    private long code2Time(int code)
    {
        return (long)(code << 15) + this.startupts;
    }

    private int time2Code(long time)
    {
        return (int)(((time - this.startupts) >> 15));
    }

    private Object returnObjectI(int flags, int val)
    {
        switch (flagRType(flags))
        {
            case b:
                return new Byte((byte)val);
            case s:
                return new Short((short)val);
            case c:
                return new Character((char)val);
            case i:
                return new Integer((int)val);
            case l:
                return new Long(val);
            case f:
                return new Float(val);
            case d:
                return new Double(val);
            case s8:
            case str:
            case obj:
                // TODO pull from object-array
                return new Object();
            case deleted:
                return null;
            default:
                throw new RuntimeException("Unrecognzied");
        }
    }

    private Object returnObjectL(int flags, long val)
    {
        switch (flagRType(flags))
        {
            case b:
                return new Byte((byte)val);
            case s:
                return new Short((short)val);
            case c:
                return new Character((char)val);
            case i:
                return new Integer((int)val);
            case l:
                return new Long(val);
            case f:
                return new Float(val);
            case d:
                return new Double(val);
            case s8:
            case str:
            case obj:
                // TODO pull from object-array
                return new Object();
            case deleted:
                return null;
            default:
                throw new RuntimeException("Unrecognzied");
        }
    }

    private KType flagRType(int flags)
    {
        int idx = flags & 0x7;
        return ktypes[idx];
    }

    private KType flagKType(int flags)
    {
        int idx = (flags >> 3) & 0x7;
        return ktypes[idx];
    }

    private int nextAddr(int p)
    {
        return p & 0x3FFFFFFF;
    }

    private int nextCode(int p)
    {
        return (p >> 30) & 0x3;
    }

    private void putI(int key, Object val, KType t)
    {
        int tidx = tablehashidx(key);
        IRecord r;
        synchronized (lock)
        {
            if (itable == null)
            {
                itable = new IRecord[1024];
            }
            r = itable[tidx];
            if (r == null)
            {
                r = new IRecord();
                itable[tidx] = r;
            }
        }
        r.lock.lock();
        try
        {
            // TODO
        } finally
        {
            r.lock.unlock();
        }
    }


    private Object getL(long key, KType t)
    {
        int ikey = (int)key;
        if ((long)ikey == key)
        {
            return getI(ikey, t);
        }
        if (ltable == null)
        {
            return null;
        }
        int tidx = tablehashidx((int)key);
        LRecord r = ltable[tidx];
        if (r == null)
        {
            return null;
        }
        return getLRecurse(key, t, r, r.next);
    }

    private int lhi(long i)
    {
        return (int)i >> 32;
    }

    private int llo(long i)
    {
        return (int)(i);
    }

    private Object getLRecurse(long key, KType kt, LRecord r, int nxt)
    {
        int code = nextCode(nxt);
        int nxtAddr = nextAddr(nxt);
        long[] data;
        switch (code)
        {
            case 0: // index node
            {
                data = r.idata;
                if (data[nxtAddr + 3] <= key)
                {
                    if (data[nxtAddr + 1] <= key)
                    {
                        if (data[nxtAddr] <= key)
                        {
                            return getLRecurse(key, kt, r, lhi(data[nxtAddr + 7]));
                        } else
                        {
                            return getLRecurse(key, kt, r, llo(data[nxtAddr + 7]));
                        }
                    } else
                    {
                        if (data[nxtAddr + 2] <= key)
                        {
                            return getLRecurse(key, kt, r, lhi(data[nxtAddr + 8]));
                        } else
                        {
                            return getLRecurse(key, kt, r, llo(data[nxtAddr + 8]));
                        }
                    }
                } else
                {
                    if (data[nxtAddr + 5] <= key)
                    {
                        if (data[nxtAddr + 4] <= key)
                        {
                            return getLRecurse(key, kt, r, lhi(data[nxtAddr + 9]));
                        } else
                        {
                            return getLRecurse(key, kt, r, llo(data[nxtAddr + 9]));
                        }
                    } else
                    {
                        if (data[nxtAddr + 6] <= key)
                        {
                            return getLRecurse(key, kt, r, lhi(data[nxtAddr + 10]));
                        } else
                        {
                            return getLRecurse(key, kt, r, llo(data[nxtAddr + 10]));
                        }
                    }
                }
            }
            case 1: // single-leaf
            {
                data = r.d1;
                if (data[nxtAddr] == key)
                {
                    int flags = lhi(data[nxtAddr + 1]);
                    long val = data[nxtAddr + 2];

                    if (kt == flagKType(flags))
                    {
                        long now = 0;
                        if (maxExp > 0)
                        {
                            long expTs = code2Time(lhi(data[nxtAddr + 3]));
                            now = System.currentTimeMillis();
                            if (expTs < now)
                            {
                                return null;
                            }
                        }
                        // not sure if this is necessary
                        if (maxIdle > 0)
                        {
                            long idleExpTs = code2Time(llo(data[nxtAddr + 3]));
                            if (now == 0)
                            {
                                now = System.currentTimeMillis();
                            }
                            if (idleExpTs < now)
                            {
                                return null;
                            }
                        }
                        Object res = returnObjectL(flags, val);
                        if (res != null && maxIdle > 0)
                        {
                            data[nxtAddr + 4] = time2Code(now + maxIdle);
                        }
                        return res;
                    }
                }
                return null;
            }
            case 2: // 3-way leaf
            {
                data = r.d3;
                for (int i = 0; i < 3; i++, nxtAddr += 4)
                {
                    if (data[nxtAddr] == key)
                    {
                        int flags = lhi(data[nxtAddr + 1]);
                        long val = data[nxtAddr + 2];

                        if (kt == flagKType(flags))
                        {
                            long now = 0;
                            if (maxExp > 0)
                            {
                                long expTs = code2Time(lhi(data[nxtAddr + 3]));
                                now = System.currentTimeMillis();
                                if (expTs < now)
                                {
                                    return null;
                                }
                            }
                            // not sure if this is necessary
                            if (maxIdle > 0)
                            {
                                long idleExpTs = code2Time(llo(data[nxtAddr + 3]));
                                if (now == 0)
                                {
                                    now = System.currentTimeMillis();
                                }
                                if (idleExpTs < now)
                                {
                                    return null;
                                }
                            }
                            Object res = returnObjectL(flags, val);
                            if (res != null && maxIdle > 0)
                            {
                                data[nxtAddr + 4] = time2Code(now + maxIdle);
                            }
                            return res;
                        }
                    }
                }
                return null;
            }
            case 3: // 5-way leaf
            {
                data = r.d5;
                for (int i = 0; i < 5; i++, nxtAddr += 4)
                {
                    if (data[nxtAddr] == key)
                    {
                        int flags = lhi(data[nxtAddr + 1]);
                        long val = data[nxtAddr + 2];

                        if (kt == flagKType(flags))
                        {
                            long now = 0;
                            if (maxExp > 0)
                            {
                                long expTs = code2Time(lhi(data[nxtAddr + 3]));
                                now = System.currentTimeMillis();
                                if (expTs < now)
                                {
                                    return null;
                                }
                            }
                            // not sure if this is necessary
                            if (maxIdle > 0)
                            {
                                long idleExpTs = code2Time(llo(data[nxtAddr + 3]));
                                if (now == 0)
                                {
                                    now = System.currentTimeMillis();
                                }
                                if (idleExpTs < now)
                                {
                                    return null;
                                }
                            }
                            Object res = returnObjectL(flags, val);
                            if (res != null && maxIdle > 0)
                            {
                                data[nxtAddr + 4] = time2Code(now + maxIdle);
                            }
                            return res;
                        }
                    }
                }
                return null;
            }
            default:
                throw new RuntimeException("Invalid next-type");
        }
    }

    private Object getObj(Object key, KType t)
    {
        if (ltable == null)
        {
            return null;
        }
        // TODO
    }


    private void putL(long key, Object val, KType t)
    {
        // TODO
    }

    private void putObj(Object key, Object val, KType t)
    {
        // TODO
    }


    private static int tablehashidx(int h)
    {
        return tablehash(h) & 0x3FF;
    }

    private static int tablehash(int h)
    {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);
        return h ^ (h >>> 16);
    }

    private static int tablehashLidx(long h)
    {
        return (int)(tablehashL(h) & 0x3FFL);
    }

    private static long tablehashL(long h)
    {
        int lo = (int)(h & 0xFFFFFFFFL);
        int hi = (int)((h >> 32) & 0xFFFFFFFFL);
        return tablehash(lo ^ hi);
    }

    private static Object lock = new Object();
    private volatile IRecord[] itable;

    static class BRecord
    {
        final Lock lock = new ReentrantLock();
        int next;

    }

    static class IRecord extends BRecord
    {

        public int[] idata;
        public int[] d1;
        public int[] d3;
        public int[] d5;
    }

    private volatile LRecord[] ltable;

    static class LRecord extends BRecord
    {
        public long[] idata;
        public long[] d1;
        public long[] d3;
        public long[] d5;
    }

    private volatile ORecord[] otable;

    static class ORecord extends BRecord
    {

    }
}
