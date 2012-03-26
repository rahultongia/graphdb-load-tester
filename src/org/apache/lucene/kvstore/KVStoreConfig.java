package org.apache.lucene.kvstore;

public class KVStoreConfig
{
    // Determines the size of cache used for "hot" keys. 
    int keyValueLRUCacheSize = DEFAULT_LRU_KEY_VALUE_CACHE_SIZE;
    public static final int DEFAULT_LRU_KEY_VALUE_CACHE_SIZE = 10000;

    public int getKeyValueLRUCacheSize()
    {
        return keyValueLRUCacheSize;
    }

    public void setKeyValueLRUCacheSize(int keyValueLRUCacheSize)
    {
        this.keyValueLRUCacheSize = keyValueLRUCacheSize;
    }
}
