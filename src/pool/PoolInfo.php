<?php

namespace mgboot\dal\pool;

use mgboot\common\traits\MapAbleTrait;

final class PoolInfo
{
    use MapAbleTrait;

    /**
     * @var int
     */
    private $maxActive = 10;

    /**
     * @var \Swoole\Atomic
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    private $currentActive;

    /**
     * @var \Swoole\Coroutine\Channel
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    private $connChan;

    /**
     * @var \Swoole\Atomic
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    private $idleCheckRunning;

    /**
     * @var \Swoole\Atomic
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    private $closed;

    private function __construct(array $settings)
    {
        $this->fromMap($settings);
    }

    public static function create(array $settings): self
    {
        return new self($settings);
    }

    /**
     * @param int $maxActive
     */
    public function setMaxActive(int $maxActive): void
    {
        $this->maxActive = $maxActive;
    }

    /**
     * @return int
     */
    public function getMaxActive(): int
    {
        return $this->maxActive;
    }

    /**
     * @return \Swoole\Atomic
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    public function getCurrentActiveAtomic(): \Swoole\Atomic
    {
        return $this->currentActive;
    }

    /**
     * @return \Swoole\Coroutine\Channel
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    public function getConnChan(): \Swoole\Coroutine\Channel
    {
        return $this->connChan;
    }

    /**
     * @return \Swoole\Atomic
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    public function getIdleCheckRunningAtomic(): \Swoole\Atomic
    {
        return $this->idleCheckRunning;
    }

    /**
     * @return \Swoole\Atomic
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    public function getClosedAtomic(): \Swoole\Atomic
    {
        return $this->closed;
    }
}
