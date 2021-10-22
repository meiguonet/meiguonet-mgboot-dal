<?php

namespace mgboot\dal\db;

use mgboot\common\constant\RandomStringType;
use mgboot\common\util\StringUtils;
use mgboot\dal\ConnectionInterface;
use PDO;
use RuntimeException;
use Throwable;

class PdoConnection extends PDO implements ConnectionInterface
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

    private function __construct(string $poolId, DbConfig $cfg)
    {
        $this->id = StringUtils::getRandomString(12, RandomStringType::ALNUM);
        $this->poolId = $poolId;

        if (!$cfg->isEnabled()) {
            throw new RuntimeException('disallow to connect to database');
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
            $port = 3306;
        }

        $username = $cfg->getUsername();

        if (is_string($cliSettings['username']) && $cliSettings['username'] !== '') {
            $username = $cliSettings['username'];
        }

        if ($username === '') {
            $username = 'root';
        }

        $password = $cfg->getPassword();

        if (is_string($cliSettings['password']) && $cliSettings['password'] !== '') {
            $password = $cliSettings['password'];
        }

        $dbname = $cfg->getDbname();

        if (is_string($cliSettings['database']) && $cliSettings['database'] !== '') {
            $dbname = $cliSettings['database'];
        }

        if ($dbname === '') {
            $dbname = 'test';
        }

        $charset = $cfg->getCharset();

        if ($charset === '') {
            $charset = 'utf8mb4';
        }

        $dsn = "mysql:dbname=$dbname;host=$host;port=$port;charset=$charset";

        $opts = [
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_STRINGIFY_FETCHES => false,
            PDO::ATTR_EMULATE_PREPARES => false,
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
        ];

        try {
            parent::__construct($dsn, $username, $password, $opts);
        } catch (Throwable $ex) {
            throw new RuntimeException($ex->getMessage());
        }
    }

    public static function create(string $poolId, DbConfig $cfg): self
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
