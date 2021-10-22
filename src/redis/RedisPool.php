<?php

namespace mgboot\dal\redis;

use mgboot\dal\pool\PoolInterface;
use mgboot\dal\pool\PoolTrait;
use Redis;
use Throwable;

final class RedisPool implements PoolInterface
{
    use PoolTrait;

    private function __construct(array $settings)
    {
        $this->init(array_merge($settings, ['poolType' => 'redis']));
    }

    public static function create(array $settings): self
    {
        return new self($settings);
    }

    private function newConnection(): ?Redis
    {
        try {
            $redis = RedisConnection::create($this->poolId, RedisCmd::getRedisConfig());
        } catch (Throwable $ex) {
            $redis = null;
        }

        return $redis instanceof Redis ? $redis : null;
    }
}
