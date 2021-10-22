<?php

namespace mgboot\dal;

interface ConnectionInterface
{
    public function getConnectionId(): string;

    public function getPoolId(): string;

    public function getLastUsedAt(): ?int;

    public function updateLastUsedAt(int $ts): void;
}
