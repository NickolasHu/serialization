import java.util.concurrent.Callable;

/**
 * Local cache
 */

public interface LocalCache<K, V> {
	/**
	 * Get value from cache
	 * @param key
	 * @param when cache miss, cacheLoader will be called. cacheLoader MUST implement call() method and load value.
	 * @return value
	 * @throws whatever call() throws
	 */
	public V get(K key, final Callable<? extends V> cacheLoader) throws Exception;
	
	/**
	 * clear all chache
	 */
	public void clear();
	
	/**
	 *  get benchmark info of cache
	 */
	public String benchmarkInfo();
}
