package org.mlm.fastcache;

/**
 * Date: May 9, 2010
 * Time: 3:00:42 PM
 */
public enum EvictionMode
{
    LRU, LFU, FIFO;

    public <K, V> Element<K, V> findBestCandidate(Element<K, V> a, Element<K, V> b)
    {
        long av = 0, bv = 0;
        switch (this)
        {
            case LRU:
                av = a.getLastUsedTS();
                bv = b.getLastUsedTS();
                break;
            case LFU:
                av = a.getUsageCount();
                bv = b.getUsageCount();
                break;
            case FIFO:
                av = a.getCreateTS();
                bv = b.getCreateTS();
                break;
        }
        if (av <= bv)
        {
            return a;
        } else
        {
            return b;
        }
    }

    public long findComparableNumeric(Element el)
    {
        switch (this)
        {
            case LRU:
                return el.getLastUsedTS();
            case LFU:
                return el.getUsageCount();
            case FIFO:
                return el.getCreateTS();
        }
        return -1;
    }

    /**
     * @param el
     * @param liveTime if non-zero this is how many millis from now until expiration
     */
    public void insertElement(Element el, long liveTime)
    {
        long now = 0;
        switch (this)
        {
            case LRU:
                el.setLastUsedTS(0);
                break;
            case FIFO:
                el.setCreateTS(now = System.currentTimeMillis());
                break;
            case LFU:
                el.setUsageCount(0);
                break;
        }

        if (liveTime > 0)
        {
            now = now == 0 ? System.currentTimeMillis() : now;
            el.setExpireTS(now + liveTime);
            el.setCreateTS(now);
        } else
        {
            el.setExpireTS(Long.MAX_VALUE);
        }
    }

    public <K, V> void useElement(Element<K, V> el)
    {
        switch (this)
        {
            case LRU:
                el.setLastUsedTS(System.currentTimeMillis());
                break;
            case FIFO:
                break;
            case LFU:
                el.incrementUsage();
                break;
        }
    }
}
