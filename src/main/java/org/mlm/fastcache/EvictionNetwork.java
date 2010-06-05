package org.mlm.fastcache;

/**
 * User: aoestreicher
 * Date: May 16, 2010
 * Time: 2:19:34 PM
 */
public interface EvictionNetwork
{
    void register(String nodeName, String cacheName, byte[] cacheId);
    void evict(byte[] cacheId, int keyHash);
}
