<?php

namespace mgboot\dal\lock;

use mgboot\common\Cast;
use mgboot\common\swoole\Swoole;
use mgboot\common\swoole\SwooleTable;
use mgboot\common\util\StringUtils;
use mgboot\dal\caching\Cache;
use mgboot\dal\ConnectionBuilder;
use Redis;
use Throwable;

final class DistributeLock
{
    /**
     * @var string
     */
    private $key;

    /**
     * @var string
     */
    private $contents;

    private function __construct(string $key)
    {
        $this->key = $key;
        $this->contents = StringUtils::getRandomString(32);
    }

    public static function create(string $key): self
    {
        return new self($key);
    }

    /**
     * @param int|string|null $waitTimeout
     * @param int|string|null $ttl
     * @return bool
     */
    public function tryLock($waitTimeout = null, $ttl = null): bool
    {
        if (is_string($waitTimeout)) {
            $waitTimeout = Cast::toDuration($waitTimeout);
        }

        if (!is_int($waitTimeout) || $waitTimeout < 1) {
            $waitTimeout = 10;
        }

        if (is_string($ttl)) {
            $ttl = Cast::toDuration($ttl);
        }

        if (!is_int($ttl) || $ttl < 1) {
            $ttl = 30;
        }

        $key = sprintf('%s@%s', Cache::buildCacheKey('redislock'), $this->key);
        $contents = $this->contents;

        if (Swoole::inCoroutineMode(true)) {
            return $this->tryLockAsync([$key, $contents, $ttl, $waitTimeout]);
        }

        $redis = ConnectionBuilder::buildRedisConnection();

        if (!is_object($redis) || !($redis instanceof Redis)) {
            return false;
        }

        $luaSha = $this->ensureLuaShaExists($redis, 'lock');

        if ($luaSha == '') {
            $redis->close();
            return false;
        }

        $ttl *= 1000;
        $execStart = time();

        while (true) {
            if (time() - $execStart > $waitTimeout) {
                break;
            }

            $result = '';

            try {
                $result = $redis->evalSha($luaSha, [$key, $contents, "$ttl"], 1);
            } catch (Throwable $ex) {
            }

            $n1 = Cast::toInt($result);

            if ($n1 >= 0) {
                $redis->close();
                return $n1 > 0;
            }

            usleep(20 * 1000);
        }

        $redis->close();
        return false;
    }

    public function release(): void
    {
        $key = sprintf('%s@%s', Cache::buildCacheKey('redislock'), $this->key);
        $contents = $this->contents;

        if (Swoole::inCoroutineMode(true)) {
            $this->releaseAsync([$key, $contents]);
            return;
        }

        $redis = ConnectionBuilder::buildRedisConnection();

        if (!is_object($redis) || !($redis instanceof Redis)) {
            return;
        }

        $luaSha = $this->ensureLuaShaExists($redis, 'unlock');

        if ($luaSha == '') {
            $redis->close();
            return;
        }

        try {
            $redis->evalSha($luaSha, [$key, $contents], 1);
        } catch (Throwable $ex) {
        } finally {
            $redis->close();
        }
    }

    private function ensureLuaShaExists(Redis $redis, string $type) : string
    {
        $cacheKey = "luasha.redislock.$type";
        $cache = Cache::store('file');
        $luaSha = $cache->get($cacheKey);

        if (is_string($luaSha) && $luaSha !== '') {
            return $luaSha;
        }

        $fpath = __DIR__ . "/redislock.$type.lua";
        $contents = file_get_contents($fpath);

        if (!is_string($contents) || $contents == '') {
            return '';
        }

        try {
            $luaSha = $redis->script('load', trim($contents));

            if (is_string($luaSha) && $luaSha !== '') {
                $cache->set($cacheKey, $luaSha);
                return $luaSha;
            }

            return '';
        } catch (Throwable $ex) {
            return '';
        }
    }

    private function tryLockAsync(array $payloads): bool
    {
        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        $wg = new \Swoole\Coroutine\WaitGroup();
        $wg->add();
        $success = false;

        go(function () use ($payloads, $wg, &$success) {
            $execStart = time();
            $tableName = SwooleTable::distributeLockTableName();

            /* @var string $key */
            /* @var string $contents */
            /* @var int $ttl */
            /* @var int $waitTimeout */
            list($key, $contents, $ttl, $waitTimeout) = $payloads;

            while (true) {
                if (time() - $execStart > $waitTimeout) {
                    break;
                }

                if (!SwooleTable::exists($tableName, $key)) {
                    $success = true;
                    SwooleTable::setValue($tableName, $key, compact('contents'));

                    /** @noinspection PhpFullyQualifiedNameUsageInspection */
                    \Swoole\Timer::after($ttl * 1000 + 100, function () use ($tableName, $key, $contents) {
                        $entry = SwooleTable::getValue($tableName, $key);

                        if (!is_array($entry) || $entry['contents'] !== $contents) {
                            return;
                        }

                        SwooleTable::remove($tableName, $key);
                    });

                    break;
                }

                usleep(20 * 1000);
            }

            $wg->done();
        });

        $wg->wait(floatval($payloads[3] + 2));
        return $success;
    }

    private function releaseAsync(array $payloads): void
    {
        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        $wg = new \Swoole\Coroutine\WaitGroup();
        $wg->add();

        go(function () use ($payloads, $wg) {
            $tableName = SwooleTable::distributeLockTableName();

            /* @var string $key */
            /* @var string $contents */
            list($key, $contents) = $payloads;

            $entry = SwooleTable::getValue($tableName, $key);

            if (!is_array($entry) || $entry['contents'] !== $contents) {
                $wg->done();
                return;
            }

            SwooleTable::remove($tableName, $key);
            $wg->done();
        });

        $wg->wait(1.0);
    }
}
