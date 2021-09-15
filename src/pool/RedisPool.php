<?php

namespace mgboot\databasex;

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
