<?php

namespace mgboot\databasex;

use PDO;

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

    private function newConnection(): ?PDO
    {
        return ConnectionBuilder::buildPdoConnection();
    }
}
