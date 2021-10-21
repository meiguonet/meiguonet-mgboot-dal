<?php

namespace mgboot\dal\db;

use mgboot\dal\Connection;
use mgboot\dal\ConnectionBuilder;
use mgboot\dal\pool\PoolInterface;
use mgboot\dal\pool\PoolTrait;

final class PdoPool implements PoolInterface
{
    use PoolTrait;

    private function __construct(array $settings)
    {
        $this->init(array_merge($settings, ['poolType' => 'pdo']));
    }

    public static function create(array $settings): self
    {
        return new self($settings);
    }

    private function newConnection(): ?Connection
    {
        $pdo = ConnectionBuilder::buildPdoConnection();

        if (!is_object($pdo)) {
            return null;
        }

        return Connection::create($this->poolId, $pdo);
    }
}
