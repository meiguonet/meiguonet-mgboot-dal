<?php

namespace mgboot\dal\db;

use mgboot\dal\pool\PoolInfo;
use mgboot\dal\pool\PoolInterface;
use mgboot\dal\pool\PoolTrait;
use PDO;
use Throwable;

final class PdoPool implements PoolInterface
{
    use PoolTrait;

    private function __construct(int $workerId, PoolInfo $poolInfo, array $settings)
    {
        $settings['poolType'] = 'pdo';
        $this->init($workerId, $poolInfo, $settings);
    }

    public static function create(int $workerId, PoolInfo $poolInfo, array $settings): self
    {
        return new self($workerId, $poolInfo, $settings);
    }

    private function newConnection(): ?PDO
    {
        $cfg = DbConfig::loadCurrent($this->workerId);

        if (!($cfg instanceof DbConfig) || !$cfg->isEnabled()) {
            return null;
        }

        try {
            $pdo = PdoConnection::create($this->poolId, $cfg);
        } catch (Throwable $ex) {
            $pdo = null;
        }

        return $pdo instanceof PDO ? $pdo : null;
    }
}
