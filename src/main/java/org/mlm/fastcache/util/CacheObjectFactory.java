package org.mlm.fastcache.util;

/**
 * Date: May 8, 2010
 * Time: 2:44:09 PM
 */
public interface CacheObjectFactory<K,V>
{
    V create(K key);
}
