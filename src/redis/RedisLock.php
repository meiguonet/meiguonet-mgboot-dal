<?php

namespace mgboot\dal\redis;

use mgboot\common\Cast;
use mgboot\dal\pool\PoolManager;
use mgboot\common\swoole\Swoole;
use mgboot\common\util\StringUtils;
use Redis;
use Throwable;

final class RedisLock
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

        $redis = PoolManager::getConnection('redis');

        if (!($redis instanceof Redis)) {
            return false;
        }

        $script = RedisCmd::loadScript('lock_try');

        if (empty($script)) {
            return false;
        }

        $key = $this->key;
        $contents = $this->contents;

        if (Swoole::inCoroutineMode(true)) {
            $wg = Swoole::newWaitGroup();
            $wg->add();
            $flag = false;

            Swoole::runInCoroutine(function () use ($redis, $script, $key, $contents, $waitTimeout, $ttl, $wg, &$flag) {
                Swoole::defer(function () use ($wg) {
                    $wg->done();
                });

                $ts1 = time();

                while (true) {
                    if (time() - $ts1 >= $waitTimeout) {
                        break;
                    }

                    try {
                        $success = Cast::toBoolean($redis->eval($script, [$key, $contents, "$ttl"], 1));
                    } catch (Throwable $ex) {
                        $success = false;
                    }

                    if ($success) {
                        $flag = true;
                        break;
                    }

                    usleep(20 * 1000);
                }
            });

            $wg->wait();
            PoolManager::releaseConnection($redis);
            return $flag;
        }

        $ts1 = time();
        $flag = false;

        while (true) {
            if (time() - $ts1 >= $waitTimeout) {
                break;
            }

            try {
                $success = Cast::toBoolean($redis->eval($script, [$this->key, $this->contents, "$ttl"], 1));
            } catch (Throwable $ex) {
                $success = false;
            }

            if ($success) {
                $flag = true;
                break;
            }

            usleep(20 * 1000);
        }

        PoolManager::releaseConnection($redis);
        return $flag;
    }

    public function release(): void
    {
        $redis = PoolManager::getConnection('redis');

        if (!($redis instanceof Redis)) {
            return;
        }

        $script = RedisCmd::loadScript('lock_release');

        if (empty($script)) {
            return;
        }

        $key = $this->key;
        $contents = $this->contents;

        try {
            $redis->eval($script, [$key, $contents], 1);
        } finally {
            PoolManager::releaseConnection($redis);
        }
    }
}
