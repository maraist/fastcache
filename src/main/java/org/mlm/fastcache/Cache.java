package org.mlm.fastcache;

/**
 * Date: May 8, 2010
 * Time: 2:30:08 PM
 */
public interface Cache<K,V>
{
    Element<K,V> get(K key);
    void put(Element<K,V> el);
    void evict(K key);
    void flushAll();
}
