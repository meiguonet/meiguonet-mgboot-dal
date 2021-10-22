<?php

namespace mgboot\dal\redis;

use mgboot\common\constant\RandomStringType;
use mgboot\common\util\StringUtils;
use mgboot\dal\ConnectionInterface;
use Redis;
use RuntimeException;
use Throwable;

class RedisConnection extends Redis implements ConnectionInterface
{
    /**
     * @var string
     */
    private $id;

    /**
     * @var string
     */
    private $poolId;

    /**
     * @var int|null
     */
    private $lastUsedAt = null;

    private function __construct(string $poolId, RedisConfig $cfg)
    {
        $this->id = StringUtils::getRandomString(12, RandomStringType::ALNUM);
        $this->poolId = $poolId;

        if (!$cfg->isEnabled()) {
            throw new RuntimeException('disallow to connect to redis server');
        }

        $cliSettings = $cfg->getCliSettings();

        if (!is_array($cliSettings)) {
            $cliSettings = [];
        }

        $host = $cfg->getHost();

        if (is_string($cliSettings['host']) && $cliSettings['host'] !== '') {
            $host = $cliSettings['host'];
        }

        if ($host === '') {
            $host = '127.0.0.1';
        }

        $port = $cfg->getPort();

        if (is_int($cliSettings['port']) && $cliSettings['port'] > 0) {
            $port = $cliSettings['port'];
        }

        if ($port < 1) {
            $port = 6379;
        }

        $password = $cfg->getPassword();

        if (is_string($cliSettings['password']) && $cliSettings['password'] !== '') {
            $password = $cliSettings['password'];
        }

        $database = $cfg->getDatabase();

        if (is_int($cliSettings['database']) && $cliSettings['database'] >= 0) {
            $database = $cliSettings['database'];
        }

        $readTimeout = $cfg->getReadTimeout();

        if ($readTimeout < 1) {
            $readTimeout = 5;
        }

        try {
            parent::__construct();

            if (!$this->connect($host, $port, 1.0, null, 0, $readTimeout)) {
                throw new RuntimeException('connect to redis server failed');
            }

            if ($password !== '' && !$this->auth($password)) {
                $this->close();
                throw new RuntimeException('redis server: auth failed');
            }

            if ($database > 0 && !$this->select($database)) {
                $this->close();
                throw new RuntimeException("redis server: fail to switch to database: db$database");
            }
        } catch (Throwable $ex) {
            throw new RuntimeException($ex->getMessage());
        }
    }

    public static function create(string $poolId, RedisConfig $cfg): self
    {
        return new self($poolId, $cfg);
    }

    public function getConnectionId(): string
    {
        return $this->id;
    }

    public function getPoolId(): string
    {
        return $this->poolId;
    }

    public function getLastUsedAt(): ?int
    {
        $ts = $this->lastUsedAt;
        return is_int($ts) ? $ts : null;
    }

    public function updateLastUsedAt(int $ts): void
    {
        $this->lastUsedAt = $ts;
    }
}
