import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.StackObjectPool;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;

/**
 * Local cache implemention
 */

public class LocalCacheImpl<K, V> implements LocalCache<K, V>{

	/**
	 * create local cache
	 * @param initialCapacity 
	 * @param expireHour 
	 * @param maxEntries 
	 * @param clazz class of cache object
	 */
	public LocalCacheImpl(int initialCapacity, long expireHour, long maxEntries, Class<V> clazz) {
		cacheImpl = CacheBuilder.newBuilder()
		.initialCapacity(initialCapacity)	 
		.maximumSize(maxEntries)			  
		.expireAfterWrite(expireHour, TimeUnit.HOURS)  
		.softValues()						 // Value as SoftReference
		.build();							 // concurrentLevel 4
		
		this.classV = clazz;
		this.maxEntries = maxEntries; 
		this.expireTime = expireHour;
	}
	
	/**
	 * Get value from cache
	 * @param key
	 * @param cacheLoader implement call() method and load value when cache miss
	 * @return value
	 * @throws whatever call() throws
	 */
	@Override
	public V get(K key, final Callable<? extends V> cacheLoader) throws Exception {
		long start = System.nanoTime();

		V obj = null;
		byte[] value = cacheImpl.getIfPresent(key);
		if (value != null) { // cache hit
			obj = decompress(value);
		} else { // cache miss
			obj = cacheLoader.call();
			if (obj != null) {
				cacheImpl.put(key, compress(obj));
			}
		}

		totalTime.addAndGet(System.nanoTime() - start);
		return obj;
	}
	
	/**
	 * clear all cache
	 */
	@Override
	public void clear() {
		cacheImpl.invalidateAll();
	}
	
	/**
	 * benchmark cache performance
	 */
	@Override
	public String benchmarkInfo() {
		final CacheStats cacheStat = cacheImpl.stats();

		double averageLoadPenalty = 0;
		double avgCompressedSize = 0; 
		if (cacheStat.requestCount() != 0) {
			averageLoadPenalty = totalTime.longValue() / (double)cacheStat.requestCount();
		}
		if (totalCompressedTimes.longValue() != 0) {
			avgCompressedSize = totalCompressedSize.longValue() / totalCompressedTimes.doubleValue(); 
		}
		StringBuilder infoBuilder = new StringBuilder("localcache benchmark:");
		infoBuilder.append("cacheSize:").append(maxEntries).append(",")
		.append("expireTime:").append(expireTime).append(",");

		infoBuilder.append("hitRate:").append(Util.asString(String.format("%.3f", cacheStat.hitRate()))).append(",")
		.append("hitCount:").append(Util.asString(cacheStat.hitCount())).append(",")
		.append("requestCount:").append(Util.asString(cacheStat.requestCount())).append(",")
		.append("averageLoadPenalty:").append(Util.asString(String.format("%.3f", averageLoadPenalty))).append(",")
		.append("avgCompressedSize:").append(Util.asString(String.format("%.3f", avgCompressedSize))).append(",")
		.append("evictionCount:").append(Util.asString(cacheStat.evictionCount())).append(",")
		.append("dataCount:").append(Util.asString(size()));

		return infoBuilder.toString();
	}
	
	// decompress bytes to object
	private V decompress(byte[] bytes) throws Exception {
		Kryo kryo = null;
		V obj = null;
		try {
			kryo = (Kryo) kryoPool.borrowObject();

			InputStream inputStream = new InflaterInputStream(new ByteArrayInputStream(bytes));
			Input input = new Input(inputStream);
			obj = kryo.readObject(input, classV);
			input.close();
		} catch (Exception e) {
			throw new CacheException("local cache decompress failed", e);
		} finally {
			kryoPool.returnObject(kryo);
		}

		return obj;
	}

	// compress object to bytes
	private byte[] compress(V value) throws Exception {
		if (value == null) {
			throw new NullPointerException("cannot compress null");
		}
		
		Kryo kryo = null;
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		try {
			kryo = (Kryo) kryoPool.borrowObject();
			OutputStream outputStream = new DeflaterOutputStream(byteStream);
			Output output = new Output(outputStream);
			kryo.writeObject(output, value);
			output.close();
			
		} catch (Exception e) {
			throw new CacheException("local cache failed", e);
		} finally {
			kryoPool.returnObject(kryo);
		}
		
		byte[] buffer = null;
		if (byteStream != null) {
			buffer = byteStream.toByteArray();
		}
		
		if (buffer != null) {
			totalCompressedSize.addAndGet(buffer.length);
			totalCompressedTimes.incrementAndGet();
		}
		
		return buffer;
	}

	// counts of cache values
	private long size() {
		return cacheImpl.size(); 
	}
	
	private Cache<K, byte[]> cacheImpl; 
	
	// benchmark info
	private AtomicLong totalCompressedSize = new AtomicLong();
	private AtomicLong totalCompressedTimes = new AtomicLong();
	private AtomicLong totalTime = new AtomicLong();
	
	// Stack Object Pool 
	private ObjectPool kryoPool = new StackObjectPool(new PoolableObjectFactory(){
		@Override
		public Object makeObject() throws Exception {
			Kryo kryo = new Kryo();
			kryo.register(classV); 
			return kryo;
		}

		@Override
		public void destroyObject(Object obj) throws Exception {}

		@Override
		public boolean validateObject(Object obj) {
			return true;
		}

		@Override
		public void activateObject(Object obj) throws Exception {}

		@Override
		public void passivateObject(Object obj) throws Exception {}

	});
	
	private Logger logger = LoggerFactory.getLogger(LocalCacheImpl.class);
	
	private final Class<V> classV; //  keep V.class
	private long maxEntries;
	private long expireTime;
}
