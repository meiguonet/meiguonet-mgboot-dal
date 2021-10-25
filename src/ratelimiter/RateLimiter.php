<?php

namespace mgboot\dal\ratelimiter;

use mgboot\common\Cast;
use mgboot\common\swoole\Swoole;
use mgboot\common\swoole\SwooleTable;
use mgboot\common\util\ArrayUtils;
use mgboot\common\util\FileUtils;
use mgboot\dal\ConnectionBuilder;
use Redis;
use Throwable;

final class RateLimiter
{
    /**
     * @var string
     */
    private $id;

    /**
     * @var int
     */
    private $count;

    /**
     * @var int
     */
    private $duration;

    /**
     * @var string
     */
    private $cacheDir;

    /**
     * @param string $id
     * @param int $count
     * @param int|string $duration
     * @param string $cacheDir
     */
    private function __construct(string $id, int $count, $duration, string $cacheDir = '')
    {
        if ($cacheDir === '') {
            $cacheDir = FileUtils::getRealpath('classpath:cache');
        }

        $this->id = $id;
        $this->count = $count;

        if (is_string($duration)) {
            $duration = Cast::toDuration($duration);
        }

        if (!is_int($duration) || $duration < 1) {
            $duration = 1;
        }

        $this->duration = $duration;
        $this->cacheDir = $cacheDir;
    }

    /**
     * @param string $id
     * @param int $count
     * @param int|string $duration
     * @param string $cacheDir
     * @return self
     */
    public static function create(string $id, int $count, $duration, string $cacheDir = ''): self
    {
        return new self($id, $count, $duration, $cacheDir);
    }

    public function getLimit(): array
    {
        if (Swoole::inCoroutineMode(true)) {
            return $this->getLimitAsync();
        }

        $redis = ConnectionBuilder::buildRedisConnection();

        if (!is_object($redis) || !($redis instanceof Redis)) {
            return [];
        }

        $luaSha = $this->ensureLuaShaExists($redis);

        if ($luaSha == '') {
            return [];
        }

        $id = sprintf(
            'ratelimiter@%s@%d@%ds',
            md5($this->id),
            $this->count,
            $this->duration
        );

        $duration = $this->duration * 1000;
        $ts1 = time();
        $ts2 = $ts1 + $this->duration + 2;

        try {
            $a1 = $redis->evalSha($luaSha, [$id, "$this->count", "$duration", "$ts2"], 1);

            if (!ArrayUtils::isList($a1) || count($a1) < 4) {
                return [];
            }

            $total = Cast::toInt($a1[1]);
            $remaining = Cast::toInt($a1[0]);

            if ($remaining >= 0) {
                return compact('total', 'remaining');
            }

            $resetAt = Cast::toInt($a1[3]);
            $retryAfter = $resetAt - $ts1;
            $resetAt = date('Y-m-d H:i:s', $resetAt);
            return compact('total', 'remaining', 'resetAt', 'retryAfter');
        } catch (Throwable $ex) {
            return [];
        } finally {
            $redis->close();
        }
    }

    public function isReachRateLimit(): bool
    {
        $map1 = $this->getLimit();
        return is_int($map1['remaining']) && $map1['remaining'] < 0;
    }

    private function getLimitAsync(): array
    {
        $tableName = SwooleTable::ratelimiterTableName();

        $key = sprintf(
            '%s_%d_%ds',
            md5($this->id),
            $this->count,
            $this->duration
        );

        if (!SwooleTable::exists($tableName, $key)) {
            $createAt = time();
            $ttl = $this->duration;
            $resetAt = $createAt + $ttl;

            $map1 = [
                'total' => $this->count,
                'remaining' => $this->count - 1,
                'resetAt' => "$resetAt",
                'createAt' => "$createAt"
            ];

            SwooleTable::setValue($tableName, $key, $map1);

            /** @noinspection PhpFullyQualifiedNameUsageInspection */
            \Swoole\Timer::after($ttl * 1000 + 100, function () use ($tableName, $key) {
                SwooleTable::remove($tableName, $key);
            });

            return $map1;
        }

        $now = time();
        $entry = SwooleTable::getValue($tableName, $key);

        if (!is_array($entry)) {
            return [
                'total' => $this->count,
                'remaining' => -1
            ];
        }

        unset($entry['createAt']);
        SwooleTable::decr($tableName, $key, 'remaining', 1);
        $entry['remaining'] -= 1;

        if ($entry['remaining'] >= 0) {
            unset($entry['resetAt']);
            return $entry;
        }

        $ts1 = (int) $entry['resetAt'];
        $entry['resetAt'] = date('Y-m-d H:i:s', $ts1);
        $map1['retryAfter'] = $ts1 - $now;
        return $entry;
    }

    private function ensureLuaShaExists(Redis $redis) : string
    {
        $cacheFile = $this->cacheDir . '/luasha.ratelimiter.dat';

        if (is_file($cacheFile)) {
            $contents = file_get_contents($cacheFile);

            if (is_string($contents) && $contents !== '') {
                return $contents;
            }
        }

        $fpath = __DIR__ . '/ratelimiter.lua';
        $contents = file_get_contents($fpath);

        if (!is_string($contents) || $contents == '') {
            return '';
        }

        try {
            $luaSha = $redis->script('load', trim($contents));

            if (is_string($luaSha) && $luaSha !== '') {
                $this->writeLuashaToCacheFile($cacheFile, $luaSha);
                return $luaSha;
            }

            return '';
        } catch (Throwable $ex) {
            return '';
        }
    }

    private function writeLuashaToCacheFile(string $cacheFile, string $contents): void
    {
        $dir = dirname($cacheFile);

        if (!is_string($dir) || $dir === '') {
            return;
        }

        if (!is_dir($dir)) {
            mkdir($dir, 0644, true);
        }

        if (!is_dir($dir) || !is_writable($dir)) {
            return;
        }

        try {
            file_put_contents($cacheFile, $contents);
        } catch (Throwable $ex) {
        }
    }
}
