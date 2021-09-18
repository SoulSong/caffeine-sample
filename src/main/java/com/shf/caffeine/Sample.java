package com.shf.caffeine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * description :
 * {@link Cache}线程安全
 *
 * @author songhaifeng
 * @date 2021/9/6 19:22
 */
@Slf4j
public class Sample {
    private static final String MOCK_KEY = "mock_key";
    private static final String MOCK_VALUE = "mock_value";
    private static final String MOCK_KEY_2 = "mock_key_2";
    private static final String MOCK_VALUE_2 = "mock_value_2";

    /**
     * 通过getIfPresent方法获取缓存值，若未缓存则返回null
     */
    @Test
    public void getIfPresentTest() {
        final Cache<String, String> cache = Caffeine.newBuilder()
                .maximumSize(100)
                .recordStats()
                .build();

        String value = cache.getIfPresent(MOCK_KEY);
        assert cache.stats().missCount() == 1;
        assert value == null;

        cache.put(MOCK_KEY, getValue(MOCK_KEY));

        value = cache.getIfPresent(MOCK_KEY);
        // 手动put添加缓存，故load仍然为0
        assert cache.stats().loadCount() == 0;
        assert cache.stats().hitCount() == 1;
        assert MOCK_VALUE.equals(value);
    }

    /**
     * 通过get方法获取缓存值：
     * 1、当缓存不存在时通过getValue获取目标值，并在getValue返回值不为null时加载至缓存。
     * 2、当缓存存在时，则直接返回缓存值；
     * 故如下示例仅首次需要执行getValue方法。
     */
    @Test
    public void getTest() {
        final Cache<String, String> cache = Caffeine.newBuilder()
                .maximumSize(100)
                .recordStats()
                .build();

        String value = cache.get(MOCK_KEY, this::getValue);
        assert cache.stats().loadCount() == 1;
        assert MOCK_VALUE.equals(value);

        value = cache.get(MOCK_KEY, this::getValue);
        assert cache.stats().loadCount() == 1;
        assert MOCK_VALUE.equals(value);
    }

    /**
     * 通过get方法获取缓存值：
     * 1、当缓存不存在时通过getValue2获取目标值，由于getValue2返回值为null，故不会加载至缓存(认为加载失败)；
     * 故如下示例中每次获取缓存值均会执行getValue2方法。
     */
    @Test
    public void getTest2() {
        final Cache<String, String> cache = Caffeine.newBuilder()
                .maximumSize(100)
                .recordStats()
                .build();

        String value = cache.get(MOCK_KEY, this::getValueNull);
        assert cache.stats().loadCount() == 1;
        assert cache.stats().missCount() == 1;
        assert cache.stats().loadFailureCount() == 1;
        assert value == null;

        value = cache.get(MOCK_KEY, this::getValueNull);
        assert cache.stats().loadCount() == 2;
        assert cache.stats().missCount() == 2;
        assert cache.stats().loadFailureCount() == 2;
        assert value == null;
    }

    /**
     * 指定cache实例为LoadingCache类型，从而支持自动加载缓存，将获取缓存的getValue方法收口至cache实例的build定义中，避免每次获取缓存均需要指定getValue
     * 其执行最终效果等价于{@link this#getTest}，但更加简洁合理。
     * 非LoadingCache实例在build方法中指定cacheLoader无效，不会被执行。
     */
    @Test
    public void autoLoadCache() {
        final LoadingCache<String, String> cache = Caffeine.newBuilder()
                .maximumSize(100)
                .recordStats()
                .build(this::getValue);

        String value = cache.get(MOCK_KEY);
        assert cache.stats().loadCount() == 1;
        assert MOCK_VALUE.equals(value);

        value = cache.get(MOCK_KEY);
        assert cache.stats().loadCount() == 1;
        assert MOCK_VALUE.equals(value);
    }

    /**
     * 其执行最终效果等价于{@link this#getTest2}
     */
    @Test
    public void autoLoadCache2() {
        final LoadingCache<String, String> cache = Caffeine.newBuilder()
                .maximumSize(100)
                .recordStats()
                .build(this::getValueNull);

        String value = cache.get(MOCK_KEY);
        assert cache.stats().loadCount() == 1;
        assert cache.stats().missCount() == 1;
        assert cache.stats().loadFailureCount() == 1;
        assert value == null;

        value = cache.get(MOCK_KEY);
        assert cache.stats().loadCount() == 2;
        assert cache.stats().missCount() == 2;
        assert cache.stats().loadFailureCount() == 2;
        assert value == null;
    }


    /**
     * 移除缓存项，对标{@link this#getTest()}验证
     */
    @Test
    public void invalidateCache() {
        final Cache<String, String> cache = Caffeine.newBuilder()
                .maximumSize(100)
                // 启用状态监控
                .recordStats()
                .build();

        String value = cache.get(MOCK_KEY, this::getValue);
        assert cache.stats().loadCount() == 1;
        assert MOCK_VALUE.equals(value);

        // 移除缓存
        cache.invalidate(MOCK_KEY);

        value = cache.get(MOCK_KEY, this::getValue);
        assert cache.stats().loadCount() == 2;
        assert MOCK_VALUE.equals(value);
    }

    /**
     * 基于容量驱逐
     */
    @Test
    public void evictNumCache() {
        final Cache<String, String> cache = Caffeine.newBuilder()
                .maximumSize(1)
                // 启用状态监控
                .recordStats()
                .build();

        String value = cache.get(MOCK_KEY, this::getValue);
        assert cache.stats().loadCount() == 1;
        assert MOCK_VALUE.equals(value);

        cache.put(MOCK_KEY_2, MOCK_VALUE);
        // 被驱逐的item不会立即被清理，通过cleanUp可以手动清理
        cache.cleanUp();

        value = cache.getIfPresent(MOCK_KEY);
        assert cache.stats().loadCount() == 1;
        assert value == null;
    }

    /**
     * 基于时间进行驱逐，当前示例为任意读写操作后超过指定时间没有再次访问则驱逐过期
     *
     * @throws InterruptedException e
     */
    @Test
    public void evictExpireAfterAccessCache() throws InterruptedException {
        final Cache<String, String> cache = Caffeine.newBuilder()
                // 一个元素在上一次读写操作后一段时间之后，在指定的时间后没有被再次访问将会被认定为过期项。
                // 在当被缓存的元素时被绑定在一个session上时，当session因为不活跃而使元素过期的情况下，这是理想的选择。
                .expireAfterAccess(5, TimeUnit.SECONDS)
                // 启用状态监控
                .recordStats()
                .build();
        cache.put(MOCK_KEY, MOCK_VALUE);

        String value = cache.getIfPresent(MOCK_KEY);
        assert MOCK_VALUE.equals(value);

        Thread.sleep(5 * 1000);

        value = cache.getIfPresent(MOCK_KEY);
        assert value == null;
    }

    /**
     * 基于时间进行驱逐，当前示例为任意写更新操作后超过指定时间则驱逐过期
     *
     * @throws InterruptedException e
     */
    @Test
    public void evictExpireAfterWriteCache() throws InterruptedException {
        System.out.println(Thread.currentThread().getId());
        final Cache<String, String> cache = Caffeine.newBuilder()
                // 一个元素将会在其创建或者最近一次被更新之后的一段时间后被认定为过期项。
                // 在对被缓存的元素的时效性存在要求的场景下，这是理想的选择。
                .expireAfterWrite(5, TimeUnit.SECONDS)
                // 启用状态监控
                .recordStats()
                // 异步执行,默认的 Executor 实现是 ForkJoinPool.commonPool()
                .removalListener((key, value, cause) ->
                        System.out.println(String.format("Key %s value %s was removed (%s) with thread %d", key, value, cause, Thread.currentThread().getId())))
                // 可以通过覆盖executor(Executor)方法自定义线程池的实现
                .executor(Executors.newSingleThreadExecutor())
                // 同步执行
                .evictionListener((key, value, cause) ->
                        System.out.println(String.format("Key %s was evicted (%s) with thread %d", key, cause, Thread.currentThread().getId())))
                .build();
        cache.put(MOCK_KEY, MOCK_VALUE);
        String value = cache.getIfPresent(MOCK_KEY);
        assert MOCK_VALUE.equals(value);

        // 未发生更新则直接被驱逐
        Thread.sleep(5 * 1000);
        value = cache.getIfPresent(MOCK_KEY);
        assert value == null;

        cache.put(MOCK_KEY, MOCK_VALUE);
        value = cache.getIfPresent(MOCK_KEY);
        assert MOCK_VALUE.equals(value);

        // 过程发生数据更新，则重新计时
        Thread.sleep(4 * 1000);
        cache.put(MOCK_KEY, MOCK_VALUE);
        Thread.sleep(4 * 1000);
        value = cache.getIfPresent(MOCK_KEY);
        assert MOCK_VALUE.equals(value);
    }


    /**
     * 基于时间进行驱逐，当前示例为任意写更新操作后超过指定时间则驱逐过期
     *
     * @throws InterruptedException e
     */
    @Test
    public void evictExpireAfterCache() throws InterruptedException {
        final LoadingCache<String, String> cache = Caffeine.newBuilder()
                // 一个元素将会在指定的时间后被认定为过期项。当被缓存的元素过期时间收到外部资源影响的时候，这是理想的选择。
                .expireAfter(new Expiry<String, String>() {
                    @Override
                    public long expireAfterCreate(String key, String value, long currentTime) {
                        long currentMillis = System.currentTimeMillis();
                        long seconds = LocalDateTime.ofInstant(Instant.ofEpochMilli(currentMillis), ZoneOffset.ofHours(8))
                                .plusSeconds(5).toEpochSecond(ZoneOffset.ofHours(8));
                        // 此处需要进行时间差计算
                        return TimeUnit.SECONDS.toNanos(seconds) - TimeUnit.MILLISECONDS.toNanos(currentMillis);
                    }

                    @Override
                    public long expireAfterUpdate(String key, String value,
                                                  long currentTime, long currentDuration) {
                        return currentDuration;
                    }

                    @Override
                    public long expireAfterRead(String key, String value,
                                                long currentTime, long currentDuration) {
                        return currentDuration;
                    }
                })
                // 启用状态监控
                .recordStats()
                .build(this::getValue);

        String value = cache.get(MOCK_KEY);
        assert MOCK_VALUE.equals(value);

        Thread.sleep(5 * 1000);
        cache.cleanUp();
        value = cache.getIfPresent(MOCK_KEY);
        assert value == null;
    }

    /**
     * 写入后自动刷新，其刷新时间并非到期就刷新，而是在到期且被查询后执行，故即使到期仍然拿到的是旧值，再下一次待刷新执行完全方可获取新值
     *
     * @throws InterruptedException e
     */
    @Test
    public void refreshAfterWriteTest() throws InterruptedException {
        final LoadingCache<String, String> cache = Caffeine.newBuilder()
                .maximumSize(100)
                // 刷新为异步执行，默认采用ForkJoinPool.commonPool
                .refreshAfterWrite(5, TimeUnit.SECONDS)
                .recordStats()
                // 自动刷新值和手动赋值采用不同值以便区别
                .build(this::getValue);

        String value = cache.get(MOCK_KEY, this::getValue2);
        assert cache.stats().loadCount() == 1;
        assert MOCK_VALUE_2.equals(value);

        Thread.sleep(6 * 1000);

        // refreshAfterWrite 将会使在写操作之后的一段时间后允许key对应的缓存元素进行刷新，
        // 但是只有在这个key被真正查询到的时候才会正式进行刷新操作。
        value = cache.get(MOCK_KEY);
        assert MOCK_VALUE_2.equals(value);
        // 读取后执行刷新，故load+1
        assert cache.stats().loadCount() == 2;

        value = cache.get(MOCK_KEY, this::getValue2);
        assert cache.stats().loadCount() == 2;
        assert MOCK_VALUE.equals(value);
    }

    private String getValue(String MOCK_KEY) {
        log.info("create value [{}] for key [{}]", MOCK_VALUE, MOCK_KEY);
        return MOCK_VALUE;
    }

    private String getValue2(String MOCK_KEY) {
        log.info("create value [{}] for key [{}]", null, MOCK_KEY);
        return MOCK_VALUE_2;
    }

    private String getValueNull(String MOCK_KEY) {
        log.info("create value [{}] for key [{}]", null, MOCK_KEY);
        return null;
    }
}
