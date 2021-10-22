<?php

namespace mgboot\dal\pool;

use mgboot\common\Cast;
use mgboot\common\swoole\Swoole;
use mgboot\common\swoole\SwooleTable;
use mgboot\common\util\StringUtils;
use mgboot\dal\ConnectionBuilder;
use mgboot\dal\ConnectionInterface;
use Redis;
use Throwable;

final class PoolManager
{
    /**
     * @var array
     */
    private static $pools = [];

    private function __construct()
    {
    }

    public static function addPool(PoolInterface $pool, ?int $workerId = null): void
    {
        if (!is_int($workerId)) {
            $workerId = Swoole::getWorkerId();
        }

        if (!is_int($workerId) || $workerId < 0) {
            return;
        }

        $poolType = StringUtils::substringBefore($pool->getPoolId(), ':');
        self::$pools["$poolType-worker$workerId"] = $pool;
    }

    public static function getPool(string $poolType, ?int $workerId = null): ?PoolInterface
    {
        if (!is_int($workerId)) {
            $workerId = Swoole::getWorkerId();
        }

        if (!is_int($workerId) || $workerId < 0) {
            return null;
        }

        $pool = self::$pools["$poolType-worker$workerId"];
        return $pool instanceof PoolInterface ? $pool : null;
    }

    /** @noinspection PhpFullyQualifiedNameUsageInspection */
    public static function getPoolChan(string $poolType, ?int $workerId = null): ?\Swoole\Coroutine\Channel
    {
        $server = Swoole::getServer();

        if (!is_object($server)) {
            return null;
        }

        if (!is_int($workerId)) {
            $workerId = Swoole::getWorkerId();
        }

        if (!is_int($workerId) || $workerId < 0) {
            return null;
        }

        $pn = "{$poolType}Chan$workerId";

        if (!property_exists($server, $pn)) {
            return null;
        }

        return $server->$pn;
    }

    public static function getConnection(string $connectionType)
    {
        if (Swoole::inCoroutineMode(true)) {
            $poolType = in_array($connectionType, ['pdo', 'redis']) ? $connectionType : 'unknow';
            $workerId = Swoole::getWorkerId();
            $pool = self::$pools["$poolType-worker$workerId"];
            $conn = null;

            if ($pool instanceof PoolInterface) {
                try {
                    $conn = $pool->take();
                } catch (Throwable $ex) {
                    $conn = null;
                }
            }

            if (!is_object($conn)) {
                switch ($connectionType) {
                    case 'pdo':
                        $conn = ConnectionBuilder::buildPdoConnection();
                        break;
                    case 'redis':
                        $conn = ConnectionBuilder::buildRedisConnection();
                        break;
                    default:
                        $conn = null;
                }
            }

            return $conn;
        }

        switch ($connectionType) {
            case 'pdo':
                return ConnectionBuilder::buildPdoConnection();
            case 'redis':
                return ConnectionBuilder::buildRedisConnection();
            default:
                return null;
        }
    }

    public static function getPoolIdFromConnection($conn): string
    {
        if (!is_object($conn) || !($conn instanceof ConnectionInterface)) {
            return '';
        }

        return $conn->getPoolId();
    }

    public static function isFromPool($conn): bool
    {
        return self::getPoolIdFromConnection($conn) !== '';
    }

    public static function releaseConnection($conn, ?Throwable $ex = null): void
    {
        if (!is_object($conn)) {
            return;
        }

        if (!($conn instanceof ConnectionInterface)) {
            if (method_exists($conn, 'close')) {
                try {
                    $conn->close();
                } catch (Throwable $ex) {
                }
            }

            unset($conn);
            return;
        }

        $poolId = $conn->getPoolId();
        $poolType = StringUtils::substringBefore($poolId, ':');
        $pool = self::getPool($poolType);

        if (!($pool instanceof PoolInterface) || $pool->getPoolId() !== $poolId) {
            return;
        }

        if ($ex instanceof Throwable && stripos($ex->getMessage(), 'gone away') !== false) {
            $pool->updateCurrentActive(-1);

            if ($pool->inDebugMode()) {
                $logger = $pool->getLogger();

                if (is_object($logger)) {
                    $workerId = Swoole::getWorkerId();
                    $id = $conn->getConnectionId();
                    $logger->info("in worker$workerId, $poolType connection[$id] has gone away, remove from pool");
                }
            }

            return;
        }

        $pool->release($conn);
    }

    public static function destroyPool(string $poolType, ?int $workerId = null, $timeout = null): void
    {
        if (!is_int($workerId)) {
            $workerId = Swoole::getWorkerId();
        }

        if (!is_int($workerId) || $workerId < 0) {
            return;
        }

        $ch = self::getPoolChan($poolType, $workerId);

        if (!is_object($ch)) {
            return;
        }

        $table = SwooleTable::getTable(SwooleTable::poolTableName());

        if (!is_object($table)) {
            return;
        }

        $key = "{$poolType}Pool$workerId";
        $table->set($key, ['closed' => 1]);
        $entry = $table->get($key);
        $maxActive = is_array($entry) && is_int($entry['maxActive']) ? $entry['maxActive'] : 0;

        if ($maxActive < 1) {
            return;
        }

        if (is_string($timeout) && $timeout !== '') {
            $timeout = Cast::toDuration($timeout);
        }

        if (!is_int($timeout) || $timeout < 1) {
            $timeout = 5;
        }

        $now = time();

        while (true) {
            if (time() - $now > $timeout) {
                break;
            }

            for ($i = 1; $i <= $maxActive; $i++) {
                $conn = $ch->pop(0.01);

                if (!is_object($conn)) {
                    continue;
                }

                if ($conn instanceof Redis) {
                    $conn->close();
                }

                unset($conn);
            }

            /** @noinspection PhpFullyQualifiedNameUsageInspection */
            \Swoole\Coroutine::sleep(0.05);
        }

        $ch->close();
    }
}
