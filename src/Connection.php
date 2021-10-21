<?php

namespace mgboot\dal;

use mgboot\common\constant\RandomStringType;
use mgboot\common\util\StringUtils;
use PDO;
use Redis;

final class Connection
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
     * @var mixed
     */
    private $realConn = null;

    /**
     * @var int|null
     */
    private $lastUsedAt = null;

    /**
     * @param string $poolId
     * @param PDO|Redis $conn
     */
    private function __construct(string $poolId, $conn)
    {
        $this->id = StringUtils::getRandomString(12, RandomStringType::ALNUM);
        $this->poolId = $poolId;

        if ($conn instanceof PDO || $conn instanceof Redis) {
            $this->realConn = $conn;
        }
    }

    /**
     * @param string $poolId
     * @param PDO|Redis $conn
     * @return self
     */
    public static function create(string $poolId, $conn): self
    {
        return new self($poolId, $conn);
    }

    /**
     * @return string
     */
    public function getId(): string
    {
        return $this->id;
    }

    /**
     * @return string
     */
    public function getPoolId(): string
    {
        return $this->poolId;
    }

    public function getPdo(): ?PDO
    {
        $pdo = $this->realConn;
        return $pdo instanceof PDO ? $pdo : null;
    }

    public function getRedis(): ?Redis
    {
        $redis = $this->realConn;
        return $redis instanceof Redis ? $redis : null;
    }

    /**
     * @return int|null
     */
    public function getLastUsedAt(): ?int
    {
        return $this->lastUsedAt;
    }

    public function updateLastUsedAt(int $ts): void
    {
        $this->lastUsedAt = $ts;
    }

    public function destroy(): void
    {
        $conn = $this->realConn;

        if (!is_object($conn)) {
            return;
        }

        if ($conn instanceof PDO) {
            unset($conn, $this->realConn);
            return;
        }

        if ($conn instanceof Redis) {
            $conn->close();
            unset($conn, $this->realConn);
        }
    }
}
