package org.mlm.fastcache;

/**
 * Date: May 8, 2010
 * Time: 2:33:22 PM
 */
public class ElementImpl<K, V> implements Element<K, V>
{
    K key;
    V value;
    long createTS;
    long lastUsedTS;
    long expireTS = Long.MAX_VALUE;
    long softExpireTS = Long.MAX_VALUE;
    int usageCount;
    int hash;
    private int version;


    public ElementImpl(K key, V value, long ts)
    {
        this.key = key;
        this.hash = key == null ? 0 : key.hashCode();
        this.hash += version;
        this.value = value;
        this.createTS = ts;
    }

    private void computeHash()
    {
        this.hash = key == null ? 0 : key.hashCode();
        this.hash += version;
    }

    public K getKey()
    {
        return key;
    }

    public void setKey(K key)
    {
        this.key = key;
        computeHash();
    }

    public V getValue()
    {
        return value;
    }

    @Override
    public int getVersion()
    {
        return version;
    }

    @Override
    public void setVersion(int v)
    {
        this.version = v;
        computeHash();
    }

    public void setValue(V value)
    {
        this.value = value;
    }

    public long getCreateTS()
    {
        return createTS;
    }

    public void setCreateTS(long createTS)
    {
        this.createTS = createTS;
    }

    public long getLastUsedTS()
    {
        return lastUsedTS;
    }

    public void setLastUsedTS(long lastUsedTS)
    {
        this.lastUsedTS = lastUsedTS;
    }

    public long getExpireTS()
    {
        return expireTS;
    }

    public void setExpireTS(long expireTS)
    {
        this.expireTS = expireTS;
    }

    public long getSoftExpireTS()
    {
        return softExpireTS;
    }

    public void setSoftExpireTS(long softExpireTS)
    {
        this.softExpireTS = softExpireTS;
    }

    public int getUsageCount()
    {
        return usageCount;
    }

    public void setUsageCount(int usageCount)
    {
        this.usageCount = usageCount;
    }

    @Override
    public boolean isSoftExpired(long now)
    {
        long ets = softExpireTS;
        if (ets != Long.MAX_VALUE && ets < now)
        {
            return true;
        } else
        {
            return isExpired(now);
        }
    }

    public boolean isSoftExpired()
    {
        long ets = softExpireTS;
        if (ets != Long.MAX_VALUE)
        {
            long now = System.currentTimeMillis();
            if (ets < now)
            {
                return true;
            } else
            {
                return isExpired(now);
            }
        } else
        {
            return isExpired();
        }
    }

    /**
     * Avoids calling system-time, and allows unit-testing
     * @param now
     * @return
     */
    @Override
    public boolean isExpired(long now)
    {
        long ets = expireTS;
        if (ets != Long.MAX_VALUE && ets < now)
        {
            return true;
        }
        return false;
    }

    public boolean isExpired()
    {
        long ets = expireTS;
        if (ets != Long.MAX_VALUE && ets < System.currentTimeMillis())
        {
            return true;
        }
        return false;
    }

    @Override
    public void incrementUsage()
    {
        usageCount++;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        ElementImpl element = (ElementImpl) o;
        if (hash != element.hash)
        {
            return false;
        }
        if (key != null ? !key.equals(element.key) : element.key != null)
        {
            return false;
        }
        if (version != element.version)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return hash;
    }
}
