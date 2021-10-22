<?php

namespace mgboot\dal\db;

use mgboot\dal\pool\PoolInterface;
use mgboot\dal\pool\PoolTrait;
use PDO;
use Throwable;

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
        try {
            $pdo = PdoConnection::create($this->poolId, DB::getDbConfig());
        } catch (Throwable $ex) {
            $pdo = null;
        }

        return $pdo instanceof PDO ? $pdo : null;
    }
}
