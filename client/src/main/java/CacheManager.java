import java.util.*;

public class CacheManager {
    private Map<String, List<String>> cache;
    private int capacity;

    public CacheManager(int capacity) {
        this.capacity = capacity;
        this.cache = new LinkedHashMap<String, List<String>>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, List<String>> eldest) {
                return size() > CacheManager.this.capacity;
            }
        };
    }
    public synchronized List<String> searchCache(String regionName) {
        return cache.getOrDefault(regionName, null);
    }
    public synchronized void addCache(String regionName, List<String> regionLoc) {
        cache.put(regionName, regionLoc);
    }
    public synchronized List<String> removeCache(String regionName) {
        return cache.remove(regionName);
    }
}