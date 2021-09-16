<?php

namespace mgboot\dal\redis;

use mgboot\common\Cast;
use mgboot\common\util\ArrayUtils;
use mgboot\dal\GobackendSettings;
use mgboot\dal\pool\PoolManager;
use mgboot\common\swoole\Swoole;
use mgboot\common\util\StringUtils;
use Redis;
use RuntimeException;
use Throwable;

/**
 * redis command like php redis
 * see https://github.com/phpredis/phpredis for detail information
 */
final class RedisCmd
{
    const EXECUTOR_TYPE_GOBACKEND = 1;
    const EXECUTOR_TYPE_PHPREDIS = 2;

    /**
     * @var RedisConfig|null
     */
    private static $redisConfig = null;

    /**
     * @var GobackendSettings|null
     */
    private static $gobackendSettings = null;

    private function __construct()
    {
    }

    public static function withRedisConfig(array $settings): void
    {
        self::$redisConfig = RedisConfig::create($settings);
    }

    public static function getRedisConfig(): RedisConfig
    {
        $cfg = self::$redisConfig;
        return $cfg instanceof RedisConfig ? $cfg : RedisConfig::create();
    }

    public static function gobackendEnabled(?array $settings = null): bool
    {
        if (ArrayUtils::isAssocArray($settings)) {
            self::$gobackendSettings = GobackendSettings::create($settings);
            return false;
        }

        $settings = self::$gobackendSettings;
        return $settings instanceof GobackendSettings && $settings->isEnabled();
    }

    public static function getGobackendSettings(): GobackendSettings
    {
        $settings = self::$gobackendSettings;
        return $settings instanceof GobackendSettings ? $settings : GobackendSettings::create();
    }

    public static function loadScript(string $scriptName): string
    {
        $filepath = str_replace("\\", '/', __DIR__ . "/scripts/$scriptName.lua");

        if (!is_file($filepath)) {
            return '';
        }

        $contents = file_get_contents($filepath);
        return is_string($contents) ? $contents : '';
    }

    public static function ping(): bool
    {
        if (self::gobackendEnabled()) {
            return self::boolResult(self::EXECUTOR_TYPE_GOBACKEND, 'PING');
        }

        return self::boolResult(self::EXECUTOR_TYPE_PHPREDIS, 'PING');
    }

    public static function info(): array
    {
        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'INFO');
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'INFO');
    }

    /* Strings */
    public static function decr(string $key): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'DECR', [$key]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'DECR', [$key]);
    }

    public static function decrBy(string $key, int $value): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'DECRBY', [$key, "$value"]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'DECRBY', [$key, "$value"]);
    }

    public static function get(string $key): string
    {
        if (self::gobackendEnabled()) {
            return self::stringResult(self::EXECUTOR_TYPE_GOBACKEND, 'GET', [$key]);
        }

        return self::stringResult(self::EXECUTOR_TYPE_PHPREDIS, 'GET', [$key]);
    }

    public static function incr(string $key): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'INCR', [$key]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'INCR', [$key]);
    }

    public static function incrBy(string $key, int $value): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'INCRBY', [$key, "$value"]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'INCRBY', [$key, "$value"]);
    }

    public static function incrByFloat(string $key, float $value): float
    {
        if (self::gobackendEnabled()) {
            return self::floatResult(self::EXECUTOR_TYPE_GOBACKEND, "INCRBYFLOAT", [$key, "$value"]);
        }

        return self::floatResult(self::EXECUTOR_TYPE_PHPREDIS, "INCRBYFLOAT", [$key, "$value"]);
    }

    /**
     * @param string[] $keys
     * @return string[]
     */
    public static function mGet(array $keys): array
    {
        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'MGET@array', $keys);
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'MGET', $keys);
    }

    public static function mSet(array $pairs): bool
    {
        $args = [];

        foreach ($pairs as $key => $val) {
            if (!is_string($key) || $key === '') {
                continue;
            }

            array_push($args, $key, Cast::toString($val));
        }

        if (empty($args)) {
            return false;
        }

        if (self::gobackendEnabled()) {
            return self::boolResult(self::EXECUTOR_TYPE_GOBACKEND, 'MSET', $args);
        }

        return self::boolResult(self::EXECUTOR_TYPE_PHPREDIS, 'MSET', $args);
    }

    public static function set(string $key, string $value): bool
    {
        if (self::gobackendEnabled()) {
            return self::boolResult(self::EXECUTOR_TYPE_GOBACKEND, 'SET', [$key, $value]);
        }

        return self::boolResult(self::EXECUTOR_TYPE_PHPREDIS, 'SET', [$key, $value]);
    }

    /**
     * @param string $key
     * @param string $value
     * @param int|string $ttl
     * @return bool
     */
    public static function setex(string $key, string $value, $ttl): bool
    {
        if (is_string($ttl)) {
            $ttl = StringUtils::toDuration($ttl);
        }

        if (self::gobackendEnabled()) {
            return self::boolResult(self::EXECUTOR_TYPE_GOBACKEND, 'SETEX', [$key, "$ttl", $value]);
        }

        return self::boolResult(self::EXECUTOR_TYPE_PHPREDIS, 'SETEX', [$key, "$ttl", $value]);
    }

    /**
     * @param string $key
     * @param string $value
     * @param int|string $ttl
     * @return bool
     */
    public static function psetex(string $key, string $value, $ttl): bool
    {
        if (is_string($ttl)) {
            $ttl = StringUtils::toDuration($ttl);
        }

        $ttl *= 1000;
        
        if (self::gobackendEnabled()) {
            return self::boolResult(self::EXECUTOR_TYPE_GOBACKEND, 'PSETEX', [$key, "$ttl", $value]);
        }

        return self::boolResult(self::EXECUTOR_TYPE_PHPREDIS, 'PSETEX', [$key, "$ttl", $value]);
    }

    public static function setnx(string $key, string $value): bool
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'SETNX', [$key, $value], -1) === 0;
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'SETNX', [$key, $value], -1) === 0;
    }
    /* end of Strings */

    /* Keys */
    public static function del(string $key): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'DEL', [$key]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'DEL', [$key]);
    }

    public static function exists(string $key): bool
    {
        if (self::gobackendEnabled()) {
            return self::boolResult(self::EXECUTOR_TYPE_GOBACKEND, 'EXISTS', [$key]);
        }

        return self::boolResult(self::EXECUTOR_TYPE_PHPREDIS, 'EXISTS', [$key]);
    }

    /**
     * @param string $key
     * @param int|string $duration
     * @return bool
     */
    public static function expire(string $key, $duration): bool
    {
        if (is_string($duration)) {
            $duration = StringUtils::toDuration($duration);
        }

        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'EXPIRE', [$key, "$duration"]) === 1;
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'EXPIRE', [$key, "$duration"]) === 1;
    }

    public static function expireAt(string $key, int $timestamp): bool
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'EXPIREAT', [$key, "$timestamp"]) === 1;
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'EXPIREAT', [$key, "$timestamp"]) === 1;
    }

    /**
     * @param string $pattern
     * @return string[]
     */
    public static function keys(string $pattern): array
    {
        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'KEYS@array', [$pattern]);
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'KEYS', [$pattern]);
    }

    public static function rename(string $key, string $newKey): bool
    {
        if (self::gobackendEnabled()) {
            return self::boolResult(self::EXECUTOR_TYPE_GOBACKEND, 'RENAME', [$key, $newKey]);
        }

        return self::boolResult(self::EXECUTOR_TYPE_PHPREDIS, 'RENAME', [$key, $newKey]);
    }

    public static function renameNx(string $key, string $newKey): bool
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'RENAMENX', [$key, $newKey]) === 1;
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'RENAMENX', [$key, $newKey]) === 1;
    }

    public static function ttl(string $key): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'TTL', [$key]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'TTL', [$key]);
    }
    /* end of Keys */

    /* Hashes */
    /**
     * @param string $key
     * @param string[] $fields
     * @return int
     */
    public static function hDel(string $key, array $fields): int
    {
        $args = [];

        foreach ($fields as $value) {
            if (!is_string($value) || $value === '') {
                continue;
            }

            $args[] = $value;
        }

        if (empty($args)) {
            return 0;
        }

        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'HDEL', array_merge([$key], $args));
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'HDEL', array_merge([$key], $args));
    }

    /**
     * @param string $key
     * @return string[]
     */
    public static function hKeys(string $key): array
    {
        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'HKEYS@array', [$key]);
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'HKEYS', [$key]);
    }

    /**
     * @param string $key
     * @return string[]
     */
    public static function hVals(string $key): array
    {
        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'HVALS@array', [$key]);
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'HVALS', [$key]);
    }

    public static function hGetAll(string $key): array
    {
        if (self::gobackendEnabled()) {
            $entries = self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'HGETALL@array', [$key]);
        } else {
            $entries = self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'HGETALL', [$key]);
        }

        $cnt = count($entries);

        if ($cnt < 1) {
            return [];
        }

        $map1 = [];

        for ($i = 0; $i < $cnt; $i += 2) {
            if ($i + 1 > $cnt - 1) {
                break;
            }

            $key = $entries[$i];
            $value = $entries[$i + 1];

            if (empty($key)) {
                continue;
            }

            $map1[$key] = $value;
        }

        return $map1;
    }

    public static function hExists(string $key, string $fieldName): bool
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'HEXISTS', [$key, $fieldName]) === 1;
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'HEXISTS', [$key, $fieldName]) === 1;
    }

    public static function hIncrBy(string $key, string $fieldName, int $num): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'HINCRBY', [$key, $fieldName, "$num"]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'HINCRBY', [$key, $fieldName, "$num"]);
    }

    public static function hIncrByFloat(string $key, string $fieldName, float $num): float
    {
        if (self::gobackendEnabled()) {
            return self::floatResult(self::EXECUTOR_TYPE_GOBACKEND, "HINCRBYFLOAT", [$key, $fieldName, "$num"]);
        }

        return self::floatResult(self::EXECUTOR_TYPE_PHPREDIS, "HINCRBYFLOAT", [$key, $fieldName, "$num"]);
    }

    public static function hMSet(array $pairs): bool
    {
        $args = [];

        foreach ($pairs as $key => $val) {
            if (!is_string($key) || $key === '') {
                continue;
            }

            array_push($args, $key, Cast::toString($val));
        }

        if (empty($args)) {
            return false;
        }

        if (self::gobackendEnabled()) {
            return self::boolResult(self::EXECUTOR_TYPE_GOBACKEND, 'HMSET', $args);
        }

        return self::boolResult(self::EXECUTOR_TYPE_PHPREDIS, 'HMSET', $args);
    }

    public static function hMGet(string $key, array $fieldNames): array
    {
        $args = [];

        foreach ($fieldNames as $val) {
            if (!is_string($val) || $val === '') {
                continue;
            }

            $args[] = $val;
        }

        $cnt = count($args);

        if ($cnt < 1) {
            return [];
        }

        if (self::gobackendEnabled()) {
            $entries = self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'HMGET@array', array_merge([$key], $args));
        } else {
            $entries = self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'HMGET', array_merge([$key], $args));
        }

        if (count($entries) !== $cnt) {
            return [];
        }

        $map1 = [];

        for ($i = 0; $i < $cnt; $i++) {
            $map1[$args[$i]] = $entries[$i];
        }

        return $map1;
    }
    /* end of Hashes */

    /* Lists */
    /**
     * @param string[] $keys
     * @param int|string $timeout
     * @return string[]
     */
    public static function blPop(array $keys, $timeout): array
    {
        $args = [];

        foreach ($keys as $key) {
            if (!is_string($key) || $key === '') {
                continue;
            }

            $args[] = $key;
        }

        if (empty($args)) {
            return [];
        }

        if (is_string($timeout)) {
            $timeout = StringUtils::toDuration($timeout);
        }

        $args[] = "$timeout";

        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'BLPOP@array', $args);
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'BLPOP', $args);
    }

    /**
     * @param string[] $keys
     * @param int|string $timeout
     * @return string[]
     */
    public static function brPop(array $keys, $timeout): array
    {
        $args = [];

        foreach ($keys as $key) {
            if (!is_string($key) || $key === '') {
                continue;
            }

            $args[] = $key;
        }

        if (empty($args)) {
            return [];
        }

        if (is_string($timeout)) {
            $timeout = StringUtils::toDuration($timeout);
        }

        $args[] = "$timeout";

        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'BRPOP@array', $args);
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'BRPOP', $args);
    }

    /**
     * @param string $srcKey
     * @param string $dstKey
     * @param int|string $timeout
     * @return string
     */
    public static function bRPopLPush(string $srcKey, string $dstKey, $timeout): string
    {
        if (is_string($timeout)) {
            $timeout = StringUtils::toDuration($timeout);
        }

        if (self::gobackendEnabled()) {
            return self::stringResult(self::EXECUTOR_TYPE_GOBACKEND, 'BRPOPLPUSH', [$srcKey, $dstKey, "$timeout"]);
        }

        return self::stringResult(self::EXECUTOR_TYPE_PHPREDIS, 'BRPOPLPUSH', [$srcKey, $dstKey, "$timeout"]);
    }

    public static function lIndex(string $key, int $idx): string
    {
        if (self::gobackendEnabled()) {
            return self::stringResult(self::EXECUTOR_TYPE_GOBACKEND, 'LINDEX', [$key, "$idx"]);
        }

        return self::stringResult(self::EXECUTOR_TYPE_PHPREDIS, 'LINDEX', [$key, "$idx"]);
    }

    public static function lGet(string $key, int $idx): string
    {
        return self::lIndex($key, $idx);
    }

    public static function lInsert(string $key, string $serachValue, string $element, bool $before = false): int
    {
        $pos = $before ? 'BEFORE' : 'AFTER';
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'LINSERT', [$key, $pos, $serachValue, $element], -1);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'LINSERT', [$key, $pos, $serachValue, $element], -1);
    }

    /**
     * @param string $key
     * @param int|null $count
     * @return string|string[]
     */
    public static function lPop(string $key, ?int $count = null)
    {
        $cmd = 'LPOP';
        $multi = false;
        $args = [$key];

        if (is_int($count) && $count > 0) {
            $cmd .= '@array';
            $multi = true;
            $args[] = "$count";
        }

        if ($multi) {
            if (self::gobackendEnabled()) {
                return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, $cmd, $args);
            }

            return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, $cmd, $args);
        }

        if (self::gobackendEnabled()) {
            return self::stringResult(self::EXECUTOR_TYPE_GOBACKEND, $cmd, $args);
        }

        return self::stringResult(self::EXECUTOR_TYPE_PHPREDIS, $cmd, $args);
    }

    public static function lPush(string $key, string... $elements): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'LPUSH', array_merge([$key], $elements));
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'LPUSH', array_merge([$key], $elements));
    }

    public static function lPushx(string $key, string... $elements): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'LPUSHX', array_merge([$key], $elements));
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'LPUSHX', array_merge([$key], $elements));
    }

    public static function lRange(string $key, int $start, int $stop): array
    {
        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'LRANGE@array', [$key, "$start", "$stop"]);
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'LRANGE', [$key, "$start", "$stop"]);
    }

    public static function lGetRange(string $key, int $start, int $stop): array
    {
        return self::lRange($key, $start, $stop);
    }

    public static function lRem(string $key, int $count, string $element): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'LREM', [$key, "$count", $element]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'LREM', [$key, "$count", $element]);
    }

    public static function lRemove(string $key, int $count, string $element): int
    {
        return self::lRem($key, $count, $element);
    }

    public static function lSet(string $key, int $idx, string $value): bool
    {
        if (self::gobackendEnabled()) {
            return self::boolResult(self::EXECUTOR_TYPE_GOBACKEND, 'LSET', [$key, "$idx", $value]);
        }

        return self::boolResult(self::EXECUTOR_TYPE_PHPREDIS, 'LSET', [$key, "$idx", $value]);
    }

    public static function lTrim(string $key, int $start, int $stop): bool
    {
        if (self::gobackendEnabled()) {
            return self::boolResult(self::EXECUTOR_TYPE_GOBACKEND, 'LTRIM', [$key, "$start", "$stop"]);
        }

        return self::boolResult(self::EXECUTOR_TYPE_PHPREDIS, 'LTRIM', [$key, "$start", "$stop"]);
    }

    public static function listTrim(string $key, int $start, int $stop): bool
    {
        return self::lTrim($key, $start, $stop);
    }

    /**
     * @param string $key
     * @param int|null $count
     * @return string|string[]
     */
    public static function rPop(string $key, ?int $count = null)
    {
        $cmd = 'RPOP';
        $multi = false;
        $args = [$key];

        if (is_int($count) && $count > 0) {
            $cmd .= '@array';
            $multi = true;
            $args[] = "$count";
        }

        if ($multi) {
            if (self::gobackendEnabled()) {
                return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, $cmd, $args);
            }

            return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, $cmd, $args);
        }

        if (self::gobackendEnabled()) {
            return self::stringResult(self::EXECUTOR_TYPE_GOBACKEND, $cmd, $args);
        }

        return self::stringResult(self::EXECUTOR_TYPE_PHPREDIS, $cmd, $args);
    }

    public static function rPopLPush(string $srcKey, string $dstKey): string
    {
        return self::fromGobackend('RPOPLPUSH', [$srcKey, $dstKey]);
    }

    public static function rPush(string $key, string... $elements): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'RPUSH', array_merge([$key], $elements));
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'RPUSH', array_merge([$key], $elements));
    }

    public static function rPushx(string $key, string... $elements): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'RPUSHX', array_merge([$key], $elements));
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'RPUSHX', array_merge([$key], $elements));
    }

    public static function lLen(string $key): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'LLEN', [$key]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'LLEN', [$key]);
    }

    public static function lSize(string $key): int
    {
        return self::lLen($key);
    }
    /* end of Lists */

    /* Sets */
    public static function sAdd(string $key, string... $members): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'SADD', array_merge([$key], $members));
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'SADD', array_merge([$key], $members));
    }

    public static function sCard(string $key): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'SCARD', [$key]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'SCARD', [$key]);
    }

    public static function sSize(string $key): int
    {
        return self::sCard($key);
    }

    public static function sDiff(string $key, string... $otherKeys): array
    {
        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'SDIFF@array', array_merge([$key], $otherKeys));
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'SDIFF', array_merge([$key], $otherKeys));
    }

    public static function sDiffStore(string $dstKey, string... $srcKeys): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'SDIFFSTORE', array_merge([$dstKey], $srcKeys));
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'SDIFFSTORE', array_merge([$dstKey], $srcKeys));
    }

    public static function sInter(string $key, string... $otherKeys): array
    {
        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'SINTER@array', array_merge([$key], $otherKeys));
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'SINTER', array_merge([$key], $otherKeys));
    }

    public static function sInterStore(string $dstKey, string... $srcKeys): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'SINTERSTORE', array_merge([$dstKey], $srcKeys));
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'SINTERSTORE', array_merge([$dstKey], $srcKeys));
    }

    public static function sIsMember(string $key, string $searchValue): bool
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'SISMEMBER', [$key, $searchValue]) === 1;
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'SISMEMBER', [$key, $searchValue]) === 1;
    }

    public static function sContains(string $key, string $searchValue): bool
    {
        return self::sIsMember($key, $searchValue);
    }

    public static function sMembers(string $key): array
    {
        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'SMEMBERS@array', [$key]);
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'SMEMBERS', [$key]);
    }

    public static function sGetMembers(string $key): array
    {
        return self::sMembers($key);
    }

    public static function sMove(string $srcKey, string $dstKey, string $searchValue): bool
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'SMOVE', [$srcKey, $dstKey, $searchValue]) === 1;
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'SMOVE', [$srcKey, $dstKey, $searchValue]) === 1;
    }

    public static function sPop(string $key, ?int $count = null): array
    {
        $args = [$key];

        if (is_int($count) && $count > 0) {
            $args[] = "$count";
        }

        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'SPOP@array', $args);
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'SPOP', $args);
    }

    public static function sRandMember(string $key, ?int $count = null): array
    {
        $args = [$key];

        if (is_int($count) && $count > 0) {
            $args[] = "$count";
        }

        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'SRANDMEMBER@array', $args);
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'SRANDMEMBER', $args);
    }

    public static function sRem(string $key): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'SREM', [$key]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'SREM', [$key]);
    }

    public static function sRemove(string $key): int
    {
        return self::sRem($key);
    }

    public static function sUnion(string $key, string... $otherKeys): array
    {
        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'SUNION@array', array_merge([$key], $otherKeys));
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'SUNION', array_merge([$key], $otherKeys));
    }

    public static function sUnionStore(string $dstKey, string... $srcKeys): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'SUNIONSTORE', array_merge([$dstKey], $srcKeys));
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'SUNIONSTORE', array_merge([$dstKey], $srcKeys));
    }
    /* end of Sets */

    /* Sorted Sets */
    /**
     * @param array $keys
     * @param int|string $timeout
     * @param bool $max
     * @return array
     */
    public static function bzPop(array $keys, $timeout, bool $max = false): array
    {
        $cmd = $max ? 'BZPOPMAX@array' : 'BZPOPMIN@array';

        if (is_string($timeout)) {
            $timeout = StringUtils::toDuration($timeout);
        }

        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, $cmd, array_merge($keys, ["$timeout"]));
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, $cmd, array_merge($keys, ["$timeout"]));
    }

    /**
     * @param string $key
     * @param int|float $score
     * @param string $value
     * @param array $options
     * @param mixed ...$otherScoreAndValues
     * @return bool
     */
    public static function zAdd(string $key, $score, string $value, array $options = [], ...$otherScoreAndValues): bool
    {
        $args = [$key];
        $supportedOptions = ['XX', 'NX', 'LT', 'GT', 'CH', 'INCR'];

        foreach ($options as $opt) {
            if (!is_string($opt) || $opt === '' || !in_array($opt, $supportedOptions)) {
                continue;
            }

            $args[] = $opt;
        }

        array_push($args, "$score", $value);

        if (!empty($otherScoreAndValues)) {
            $cnt = count($otherScoreAndValues);

            for ($i = 0; $i < $cnt; $i += 2) {
                if ($i + 1 > $cnt - 1) {
                    break;
                }

                $otherScore = $otherScoreAndValues[$i];

                if (!is_int($otherScore)) {
                    continue;
                }

                $otherValue = Cast::toString($otherScoreAndValues[$i + 1]);

                if ($otherValue === '') {
                    continue;
                }

                array_push($args, "$otherScore", $otherValue);
            }
        }

        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'ZADD', $args, -1) >= 0;
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'ZADD', $args, -1) >= 0;
    }

    public static function zCard(string $key): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'ZCARD', [$key]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'ZCARD', [$key]);
    }

    public static function zSize(string $key): int
    {
        return self::zCard($key);
    }

    /**
     * @param string $key
     * @param int|float $start
     * @param int|float $end
     * @return int
     */
    public static function zCount(string $key, $start, $end): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'ZCOUNT', [$key, "$start", "$end"]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'ZCOUNT', [$key, "$start", "$end"]);
    }

    public static function zIncrBy(string $key, string $searchValue, int $num): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'ZINCRBY', [$key, "$num", $searchValue]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'ZINCRBY', [$key, "$num", $searchValue]);
    }

    public static function zPop(string $key, ?int $count = null, bool $max = false): array
    {
        $cmd = $max ? 'ZPOPMAX@array' : 'ZPOPMIN@array';
        $args = [$key];

        if (is_int($count) && $count > 0) {
            $args[] = "$count";
        }

        if (self::gobackendEnabled()) {
            return self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, $cmd, $args);
        }

        return self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, $cmd, $args);
    }

    /**
     * @param string $key
     * @param int|float $min
     * @param int|float $max
     * @param array $options
     * @return array
     */
    public static function zRange(string $key, $min, $max, array $options = []): array
    {
        $args = [$key, "$min", "$max", 'BYSCORE'];

        if (Cast::toBoolean('rev')) {
            $args[] = 'REV';
        }

        if (is_array($options['limit'])) {
            list($offset, $count) = $options['limit'];
            $offset = Cast::toInt($offset);
            $count = Cast::toInt($count);

            if ($offset >= 0 && $count > 0) {
                array_push($args, 'LIMIT', "$offset", "$count");
            }
        }

        $withscores = Cast::toBoolean($options['withscores']);

        if ($withscores) {
            $args[] = 'WITHSCORES';
        }

        if (self::gobackendEnabled()) {
            $entries = self::arrayResult(self::EXECUTOR_TYPE_GOBACKEND, 'ZRANGE@array', $args);
        } else {
            $entries = self::arrayResult(self::EXECUTOR_TYPE_PHPREDIS, 'ZRANGE', $args);
        }

        if (!$withscores) {
            return $entries;
        }

        $cnt = count($entries);
        $list = [];

        for ($i = 0; $i < $cnt; $i += 2) {
            if ($i + 1 > $cnt - 1) {
                break;
            }

            $list[] = [
                'score' => $entries[$i + 1],
                'element' => $entries[$i]
            ];
        }

        return $list;
    }

    /**
     * @param string $key
     * @param int|float $min
     * @param int|float $max
     * @param array $options
     * @return array
     */
    public static function zRangeByScore(string $key, $min, $max, array $options = []): array
    {
        return self::zRange($key, $min, $max, $options);
    }

    /**
     * @param string $key
     * @param int|float $min
     * @param int|float $max
     * @param array $options
     * @return array
     */
    public static function zRevRangeByScore(string $key, $min, $max, array $options = []): array
    {
        return self::zRange($key, $min, $max, $options);
    }

    public static function zRank(string $key, string $searchValue): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'ZRANK', [$key, $searchValue]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'ZRANK', [$key, $searchValue]);
    }

    public static function zRevRank(string $key, string $searchValue): int
    {
        return self::zRank($key, $searchValue);
    }

    public static function zRem(string $key, string... $members): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'ZREM', array_merge([$key], $members));
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'ZREM', array_merge([$key], $members));
    }

    public static function zDelete(string $key, string... $members): int
    {
        return self::zRem($key, ...$members);
    }

    public static function zRemove(string $key, string... $members): int
    {
        return self::zRem($key, ...$members);
    }

    public static function zRemRangeByRank(string $key, int $start, int $end): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'ZREMRANGEBYRANK', [$key, "$start", "$end"]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'ZREMRANGEBYRANK', [$key, "$start", "$end"]);
    }

    public static function zDeleteRangeByRank(string $key, int $start, int $end): int
    {
        return self::zRemRangeByRank($key, $start, $end);
    }

    /**
     * @param string $key
     * @param int|float $start
     * @param int|float $end
     * @return int
     */
    public static function zRemRangeByScore(string $key, $start, $end): int
    {
        if (self::gobackendEnabled()) {
            return self::intResult(self::EXECUTOR_TYPE_GOBACKEND, 'ZREMRANGEBYSCORE', [$key, "$start", "$end"]);
        }

        return self::intResult(self::EXECUTOR_TYPE_PHPREDIS, 'ZREMRANGEBYSCORE', [$key, "$start", "$end"]);
    }

    /**
     * @param string $key
     * @param int|float $start
     * @param int|float $end
     * @return int
     */
    public static function zDeleteRangeByScore(string $key, $start, $end): int
    {
        return self::zRemRangeByScore($key, $start, $end);
    }

    /**
     * @param string $key
     * @param int|float $start
     * @param int|float $end
     * @return int
     */
    public static function zRemoveRangeByScore(string $key, $start, $end): int
    {
        return self::zRemRangeByScore($key, $start, $end);
    }

    /**
     * @param string $key
     * @param int|float $min
     * @param int|float $max
     * @param array $options
     * @return array
     */
    public static function zRevRange(string $key, $min, $max, array $options = []): array
    {
        $options['rev'] = true;
        return self::zRange($key, $min, $max, $options);
    }

    public static function zScore(string $key, string $searchValue): float
    {
        if (self::gobackendEnabled()) {
            return self::floatResult(self::EXECUTOR_TYPE_GOBACKEND, 'ZSCORE', [$key, $searchValue]);
        }

        return self::floatResult(self::EXECUTOR_TYPE_PHPREDIS, 'ZSCORE', [$key, $searchValue]);
    }
    /* end of Sorted Sets */

    private static function stringResult(int $executorType, string $cmd, ?array $args = null): string
    {
        switch ($executorType) {
            case self::EXECUTOR_TYPE_GOBACKEND:
                try {
                    return self::handleNilString(self::fromGobackend($cmd, $args));
                } catch (Throwable $ex) {
                    throw new RuntimeException($ex->getMessage());
                }
            default:
                try {
                    $result = self::fromPhpRedis($cmd, $args);
                    return is_string($result) ? $result : '';
                } catch (Throwable $ex) {
                    throw new RuntimeException($ex->getMessage());
                }
        }
    }

    private static function boolResult(int $executorType, string $cmd, ?array $args = null): bool
    {
        switch ($executorType) {
            case self::EXECUTOR_TYPE_GOBACKEND:
                try {
                    $s1 = self::fromGobackend($cmd, $args);

                    if ($s1 === 'OK') {
                        return true;
                    }

                    return Cast::toBoolean($s1);
                } catch (Throwable $ex) {
                    throw new RuntimeException($ex->getMessage());
                }
            default:
                try {
                    $result = self::fromPhpRedis($cmd, $args);

                    if (is_string($result) && strtoupper($result) === 'OK') {
                        return true;
                    }

                    return Cast::toBoolean($result);
                } catch (Throwable $ex) {
                    throw new RuntimeException($ex->getMessage());
                }
        }
    }

    public static function intResult(int $executorType, string $cmd, ?array $args = null, int $default = 0): int
    {
        switch ($executorType) {
            case self::EXECUTOR_TYPE_GOBACKEND:
                try {
                    $s1 = self::fromGobackend($cmd, $args);
                    $s1 = preg_replace('/^[0-9]-/+', '', $s1);
                    return Cast::toInt($s1, $default);
                } catch (Throwable $ex) {
                    throw new RuntimeException($ex->getMessage());
                }
            default:
                try {
                    $result = self::fromPhpRedis($cmd, $args);
                    return Cast::toInt($result, $default);
                } catch (Throwable $ex) {
                    throw new RuntimeException($ex->getMessage());
                }
        }
    }

    public static function floatResult(int $executorType, string $cmd, ?array $args = null, float $default = 0.0): float
    {
        switch ($executorType) {
            case self::EXECUTOR_TYPE_GOBACKEND:
                try {
                    $s1 = self::fromGobackend($cmd, $args);
                    $s1 = preg_replace('/^[0-9]-/+', '', $s1);
                    return Cast::toFloat($s1, $default);
                } catch (Throwable $ex) {
                    throw new RuntimeException($ex->getMessage());
                }
            default:
                try {
                    $result = self::fromPhpRedis($cmd, $args);
                    return Cast::toFloat($result, $default);
                } catch (Throwable $ex) {
                    throw new RuntimeException($ex->getMessage());
                }
        }
    }

    public static function arrayResult(int $executorType, string $cmd, ?array $args = null): array
    {
        switch ($executorType) {
            case self::EXECUTOR_TYPE_GOBACKEND:
                try {
                    $s1 = self::fromGobackend($cmd, $args);
                    $values = explode('@^sep^@', $s1);

                    foreach ($values as $i => $val) {
                        $values[$i] = self::handleNilString($val);
                    }

                    return $values;
                } catch (Throwable $ex) {
                    throw new RuntimeException($ex->getMessage());
                }
            default:
                try {
                    $result = self::fromPhpRedis($cmd, $args);
                    return is_array($result) ? $result : [];
                } catch (Throwable $ex) {
                    throw new RuntimeException($ex->getMessage());
                }
        }
    }

    private static function fromGobackend(string $cmd, ?array $args = null): string
    {
        if (strpos($cmd, '@') !== false) {
            $parts = explode('@', $cmd);
            $cmd = strtoupper($parts[0]) . '@' . $parts[1];
        } else {
            $cmd = strtoupper($cmd);
        }

        $settings = self::getGobackendSettings();

        if (!$settings->isEnabled()) {
            throw new RuntimeException('RedisCmd: fail to load gobackend settings');
        }

        $host = $settings->getHost();

        if (empty($host)) {
            throw new RuntimeException('RedisCmd: gobackend settings: host not specified]');
        }

        $port = $settings->getPort();

        if ($port < 1) {
            throw new RuntimeException('RedisCmd: gobackend settings: port not specified');
        }

        $msg = "@@redis:@@cmd:$cmd";
        $sb = [];

        if (is_array($args)) {
            foreach ($args as $arg) {
                if (!is_string($arg) || $arg === '') {
                    continue;
                }

                $sb[] = $arg;
            }
        }

        if (!empty($sb)) {
            $msg .= '@^@' . implode('@^@', $sb);
        }
        
        if (Swoole::inCoroutineMode(true)) {
            return self::fromGobackendAsync([$host, $port, $msg]);
        }

        $fp = fsockopen($host, $port);

        if (!is_resource($fp)) {
            throw new RuntimeException('RedisCmd: fail to connect to gobackend');
        }

        try {
            fwrite($fp, $msg);
            stream_set_timeout($fp, 5);
            $result = '';
            
            while (!feof($fp)) {
                $buf = fread($fp, 2 * 1024 * 1024);
                $info = stream_get_meta_data($fp);

                if ($info['timed_out']) {
                    throw new RuntimeException('RedisCmd: read timeout from gobackend');
                }
                
                if (is_string($buf) && $buf !== '') {
                    $result .= $buf;
                }
            }

            if (!is_string($result) || $result === '') {
                throw new RuntimeException('RedisCmd: no contents read from gobackend');
            }

            return str_replace('@^@end', '', $result);
        } catch (Throwable $ex) {
            throw new RuntimeException($ex->getMessage());
        } finally {
            fclose($fp);
        }
    }
    
    private static function fromGobackendAsync(array $payloads): string
    {
        $wg = Swoole::newWaitGroup();
        $wg->add();
        $parts = ['', ''];
        
        Swoole::runInCoroutine(function () use ($payloads, $wg, &$parts) {
            Swoole::defer(function () use ($wg) {
                $wg->done();
            });

            list($host, $port, $msg) = $payloads;
            $fp = fsockopen($host, $port);

            if (!is_resource($fp)) {
                $parts[1] = 'RedisCmd: fail to connect to gobackend';
                return;
            }

            try {
                fwrite($fp, $msg);
                stream_set_timeout($fp, 5);
                $result = '';

                while (!feof($fp)) {
                    $buf = fread($fp, 2 * 1024 * 1024);
                    $info = stream_get_meta_data($fp);

                    if ($info['timed_out']) {
                        $parts[1] = 'RedisCmd: read timeout from gobackend';
                        return;
                    }

                    if (is_string($buf) && $buf !== '') {
                        $result .= $buf;
                    }
                }

                if (!is_string($result) || $result === '') {
                    $parts[1] = 'RedisCmd: no contents read from gobackend';
                } else {
                    $parts[0] = str_replace('@^@end', '', $result);
                }
            } catch (Throwable $ex) {
                $parts[1] = $ex->getMessage();
            } finally {
                fclose($fp);
            }
        });
        
        $wg->wait();
        list($result, $errorTips) = $parts;
        
        if (!empty($errorTips)) {
            throw new RuntimeException($errorTips);
        }
        
        return $result;
    }

    private static function fromPhpRedis(string $cmd, ?array $args = null)
    {
        $redis = PoolManager::getConnection('redis');

        if (!($redis instanceof Redis)) {
            throw new RuntimeException('RedisCmd: fail to connect to redis server');
        }

        if (strpos($cmd, '@') !== false) {
            $cmd = StringUtils::substringBefore($cmd, '@');
        }

        $cmd = strtolower($cmd);

        try {
            if (empty($args)) {
                $result = call_user_func([$redis, $cmd]);
            } else {
                $result = call_user_func_array([$redis, $cmd], $args);
            }

            return $result;
        } catch (Throwable $ex) {
            throw new RuntimeException($ex->getMessage());
        } finally {
            PoolManager::releaseConnection($redis);
        }
    }

    private static function handleNilString(string $str): string
    {
        $nullValues = ['(nil)', 'nil', 'null', 'NULL'];
        return in_array($str, $nullValues) ? '' : $str;
    }
}
