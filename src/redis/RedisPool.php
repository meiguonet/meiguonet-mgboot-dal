<?php

namespace mgboot\dal\redis;

use mgboot\dal\ConnectionBuilder;
use mgboot\dal\pool\PoolInterface;
use mgboot\dal\pool\PoolTrait;
use Redis;

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
        return ConnectionBuilder::buildRedisConnection();
    }
}
