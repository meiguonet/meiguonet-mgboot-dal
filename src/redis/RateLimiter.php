<?php

namespace mgboot\dal\redis;

use mgboot\common\Cast;
use mgboot\dal\pool\PoolManager;
use Redis;
use Throwable;

final class RateLimiter
{
    /**
     * @var string
     */
    private $name;

    /**
     * @var int
     */
    private $duration;

    /**
     * @var int
     */
    private $limitCount;

    /**
     * @param string $name
     * @param int|string $duration
     * @param int $limitCount
     */
    private function __construct(string $name, $duration, int $limitCount)
    {
        if (is_string($duration)) {
            $duration = Cast::toDuration($duration);
        }
        
        if (!is_int($duration) || $duration < 1) {
            $duration = 1;
        }
        
        $this->name = $name;
        $this->duration = $duration;
        $this->limitCount = $limitCount;
    }

    /**
     * @param string $name
     * @param int|string $duration
     * @param int $limitCount
     * @return static
     */
    public static function create(string $name, $duration, int $limitCount): self
    {
        return new self($name, $duration, $limitCount);
    }
    
    public function acquire(): bool
    {
        $redis = PoolManager::getConnection('redis');

        if (!($redis instanceof Redis)) {
            return false;
        }

        $script = RedisCmd::loadScript('rate_limiter');

        if (empty($script)) {
            return false;
        }
        
        $name = $this->name;
        $duration = $this->duration;
        $now = time();
        $expiredAt = $now + $duration;

        try {
            $result = $redis->eval($script, [$name, "$now", "$expiredAt"], 1);
            $n1 = Cast::toInt($result, -1);

            if ($n1 < 1) {
                $n1 = PHP_INT_MAX;
            }
        } catch (Throwable $ex) {
            $n1 = PHP_INT_MAX;
        } finally {
            PoolManager::releaseConnection($redis);
        }

        return $n1 <= $this->limitCount;
    }
}
