<?php

namespace mgboot\dal\caching;

use mgboot\common\util\FileUtils;
use mgboot\common\util\StringUtils;

final class Cache
{
    /**
     * @var string
     */
    private static $_cacheKeyPrefix = '';

    /**
     * @var string
     */
    private static $_defaultStore = 'redis';

    /**
     * @var array
     */
    private static $stores = [];

    private function __construct()
    {
    }

    public static function cacheKeyPrefix(?string $prefix = null): string
    {
        if (is_string($prefix)) {
            self::$_cacheKeyPrefix = $prefix;
            return '';
        }

        return self::$_cacheKeyPrefix;
    }

    public static function buildCacheKey(string $cacheKey): string
    {
        $prefix = rtrim(self::cacheKeyPrefix(), '.');

        if (empty($prefix)) {
            return $cacheKey;
        }

        return $prefix . StringUtils::ensureLeft($cacheKey, '.');
    }

    public static function withFileCache(string $cacheDir, string $storeName = 'file'): void
    {
        $cacheDir = FileUtils::getRealpath($cacheDir);
        $cacheDir = str_replace("\\", '/', $cacheDir);

        if ($cacheDir !== '/') {
            $cacheDir = rtrim($cacheDir);
        }

        if (!is_dir($cacheDir) || !is_writable($cacheDir)) {
            return;
        }

        self::$stores[] = [
            'name' => $storeName,
            'instance' => new FileCache($cacheDir)
        ];
    }

    public static function withRedisCache(string $storeName = 'redis'): void
    {
        self::$stores[] = [
            'name' => $storeName,
            'instance' => new RedisCache()
        ];
    }

    public static function withSwooleCache(string $storeName = 'swoole'): void
    {
        self::$stores[] = [
            'name' => $storeName,
            'instance' => new SwooleCache()
        ];
    }

    public static function defaultStore(?string $name = null): CacheInterface
    {
        if (is_string($name)) {
            if ($name !== '') {
                self::$_defaultStore = $name;
            }

            return new NoopCache();
        }

        $cache = null;
        $storeName = self::$_defaultStore;

        foreach (self::$stores as $store) {
            if ($store['name'] === $storeName) {
                $cache = $store['instance'];
                break;
            }
        }

        return $cache instanceof CacheInterface ? $cache : new NoopCache();
    }

    public static function store(string $name = ''): CacheInterface
    {
        if (empty($name)) {
            return self::defaultStore();
        }

        $cache = null;

        foreach (self::$stores as $store) {
            if ($store['name'] === $name) {
                $cache = $store['instance'];
                break;
            }
        }

        return $cache instanceof CacheInterface ? $cache : new NoopCache();
    }

    public static function get(string $key, $default = null)
    {
        return self::defaultStore()->get($key, $default);
    }

    public static function set(string $key, $value, ?int $ttl = null): bool
    {
        return self::defaultStore()->set($key, $value, $ttl);
    }

    public static function delete(string $key): bool
    {
        return self::defaultStore()->delete($key);
    }

    public static function has(string $key): bool
    {
        return self::defaultStore()->has($key);
    }
}
