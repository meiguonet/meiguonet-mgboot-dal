<?php

namespace mgboot\dal\pool;

use Psr\Log\LoggerInterface;

interface PoolInterface
{
    public function inDebugMode(?bool $flag = null): bool;

    public function withLogger(LoggerInterface $logger): void;

    public function getLogger(): ?LoggerInterface;

    public function getPoolId(): string;

    public function run(): void;

    /**
     * @param int|float|null $timeout
     * @return mixed
     */
    public function take($timeout = null);

    public function release($conn): void;

    public function updateCurrentActive(int $num): void;

    /**
     * @param int|string|null $timeout
     */
    public function destroy($timeout = null): void;
}
