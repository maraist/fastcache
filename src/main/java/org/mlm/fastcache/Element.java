package org.mlm.fastcache;

/**
 * User: maraist
 * Date: May 8, 2010
 * Time: 2:30:23 PM
 */
public interface Element<K,V> {
    K getKey();
    V getValue();
    int getVersion();
    void setVersion(int v);
    void setValue(V value);
    long getCreateTS();
    void setCreateTS(long ts);
    long getLastUsedTS();
    void setLastUsedTS(long ts);
    long getExpireTS();
    void setExpireTS(long ts);
    int getUsageCount();
    void incrementUsage();
}
