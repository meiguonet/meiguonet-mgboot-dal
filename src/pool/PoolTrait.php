<?php

namespace mgboot\dal\pool;

use mgboot\common\Cast;
use mgboot\common\constant\DateTimeFormat;
use mgboot\common\constant\Regexp;
use mgboot\common\swoole\Swoole;
use mgboot\common\swoole\SwooleTable;
use mgboot\common\util\ArrayUtils;
use mgboot\common\util\StringUtils;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Throwable;

trait PoolTrait
{
    /**
     * @var bool
     */
    private $isInDebugMode = false;

    /**
     * @var LoggerInterface|null
     */
    private $logger = null;

    /**
     * @var string
     */
    private $poolId = '';

    /**
     * @var int
     */
    private $minActive = 10;

    /**
     * @var int
     */
    private $maxActive = 10;

    /**
     * @var float
     */
    private $takeTimeout = 3.0;

    /**
     * @var int
     */
    private $maxIdleTime = 1800;

    /**
     * @var int
     */
    private $idleCheckInterval = 10;

    /**
     * @var \Swoole\Coroutine\Channel|null
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    private $connChan = null;

    public function inDebugMode(?bool $flag = null): bool
    {
        if (is_bool($flag)) {
            $this->isInDebugMode = $flag === true;
            return false;
        }

        return $this->isInDebugMode;
    }

    public function withLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
    }

    /**
     * @return LoggerInterface|null
     */
    public function getLogger(): ?LoggerInterface
    {
        return $this->logger;
    }

    public function getPoolId(): string
    {
        return $this->poolId;
    }

    public function run(): void
    {
        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        $ch = new \Swoole\Coroutine\Channel($this->maxActive);
        $currentActive = 0;

        for ($i = 1; $i <= $this->minActive; $i++) {
            $conn = $this->buildConnectionInternal();

            if (!is_object($conn)) {
                continue;
            }

            $currentActive++;
            $this->markConnFromPool($conn);
            $ch->push($conn);
        }

        $this->connChan = $ch;
        $key = $this->poolId;
        $table = SwooleTable::getTable(SwooleTable::poolTableName());

        if (is_object($table)) {
            $table->set($key, [
                'poolId' => '',
                'currentActive' => $currentActive,
                'idleCheckRunning' => 0,
                'lastUsedAt' => ''
            ]);
        }

        $logger = $this->logger;

        if (is_object($logger) && $this->inDebugMode()) {
            $workerId = Swoole::getWorkerId();
            $poolType = StringUtils::substringBefore($this->poolId, ':');

            $msg = sprintf(
                'in worker%d, %s pool[minActive=%d, maxActive=%d, currentActive=%d, takeTimeout=%ds, maxIdleTime=%ds, idleCheckInterval=%ds] is running',
                $workerId,
                $poolType,
                $this->minActive,
                $this->maxActive,
                $currentActive,
                $this->takeTimeout,
                $this->maxIdleTime,
                $this->idleCheckInterval
            );

            $logger->info($msg);
        }

        $this->runIdleChecker();
    }

    public function take($timeout = null)
    {
        $ex1 = new RuntimeException('fail to take connection from connection pool: ' . get_class($this));

        if ($this->idleCheckRunning()) {
            $conn = $this->buildConnectionInternal();

            if (!is_object($conn)) {
                throw $ex1;
            }

            return $conn;
        }

        $ch = $this->connChan;

        if (!is_object($ch)) {
            throw $ex1;
        }

        $conn = $ch->pop(0.01);

        if (is_object($conn)) {
            $this->connLastUsedAt($conn, time());
            $this->logTakeSuccess($conn);
            return $conn;
        }

        if ($this->getCurrentActive() < $this->maxActive) {
            $conn = $this->buildConnectionInternal();

            if (is_object($conn)) {
                $this->markConnFromPool($conn);
                $this->connLastUsedAt($conn, time());
                $this->updateCurrentActive(1);
                $this->logTakeSuccess($conn);
                return $conn;
            }
        }

        $timeout = Cast::toFloat($timeout);

        if ($timeout < 1.0) {
            $timeout = $this->takeTimeout;
        }

        $conn = $ch->pop($timeout);

        if (!is_object($conn)) {
            $this->logTakeFail();
            throw $ex1;
        }

        $this->connLastUsedAt($conn, time());
        $this->logTakeSuccess($conn);
        return $conn;
    }

    public function release($conn): void
    {
        if (!is_object($conn)) {
            return;
        }

        $ch = $this->connChan;

        if (!is_object($ch)) {
            return;
        }

        $poolId = PoolManager::getPoolIdFromConnection($conn);

        if ($poolId !== $this->poolId) {
            return;
        }

        $ch->push($conn);
        $this->logReleaseSuccess($conn);
    }

    public function updateCurrentActive(int $num): void
    {
        $table = SwooleTable::getTable(SwooleTable::poolTableName());

        if (!is_object($table)) {
            return;
        }

        $key = $this->poolId;

        if ($num > 0) {
            $table->incr($key, 'currentActive', $num);
        } else {
            $n1 = $this->getCurrentActive();
            $n2 = abs($num);

            if ($n1 - $n2 < 0) {
                $table->set($key, ['currentActive' => 0]);
            } else {
                $table->decr($key, 'currentActive', $n2);
            }
        }
    }

    public function destroy($timeout = null): void
    {
        $ch = $this->connChan;

        if (!is_object($ch)) {
            return;
        }

        $_timeout = 5;

        if (is_int($timeout) && $timeout > 0) {
            $_timeout = $timeout;
        } else if (is_string($timeout) && $timeout !== '') {
            $timeout = Cast::toDuration($timeout);

            if ($timeout > 0) {
                $_timeout = $timeout;
            }
        }

        $ts = time();

        while (true) {
            if (time() - $ts > $_timeout) {
                break;
            }

            for ($i = 1; $i <= $this->maxActive; $i++) {
                $conn = $ch->pop(0.01);
                
                if (!is_object($conn)) {
                    continue;
                }
                
                if (method_exists($conn, 'close')) {
                    $conn->close();
                }

                unset($conn);
            }
        }
    }

    private function runIdleChecker(): void
    {
        $ch = $this->connChan;

        if (!is_object($ch)) {
            return;
        }

        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        \Swoole\Timer::tick($this->idleCheckInterval * 1000, function () use ($ch) {
            $this->idleCheckRunning(true);
            $now = time();
            $connections = [];

            while (!$ch->isEmpty()) {
                $conn = $ch->pop(0.01);

                if (!is_object($conn)) {
                    continue;
                }

                $lastUsedAt = $this->connLastUsedAt($conn);

                if ($lastUsedAt < 1) {
                    $this->connLastUsedAt($conn, time());
                    $connections[] = $conn;
                    continue;
                }

                if ($now - $lastUsedAt >= $this->maxIdleTime) {
                    $this->logWithRemoveEvent($conn);

                    if (method_exists($conn, 'close')) {
                        try {
                            $conn->close();
                        } catch (Throwable $ex) {
                        }

                        unset($conn);
                    }

                    $this->updateCurrentActive(-1);
                    continue;
                }

                $connections[] = $conn;
            }

            foreach ($connections as $conn) {
                $ch->push($conn);
            }

            $this->idleCheckRunning(false);
        });
    }

    private function init(array $settings): void
    {
        $settings = $this->handleSettings($settings);
        $minActive = 10;

        if (is_int($settings['minActive']) && $settings['minActive'] > 0) {
            $minActive = $settings['minActive'];
        }

        $maxActive = $minActive;

        if (is_int($settings['maxActive']) && $settings['maxActive'] > $minActive) {
            $maxActive = $settings['maxActive'];
        }

        $takeTimeout = 3.0;
        $n1 = Cast::toFloat($settings['takeTimeout']);

        if ($n1 > 0) {
            $takeTimeout = $n1;
        }

        if ($takeTimeout < 1.0) {
            $takeTimeout = 1.0;
        }

        $maxIdleTime = 1800;

        if (is_int($settings['maxIdleTime']) && $settings['maxIdleTime'] > 0) {
            $maxIdleTime = $settings['maxIdleTime'];
        } else if (is_string($settings['maxIdleTime']) && $settings['maxIdleTime'] !== '') {
            $n1 = StringUtils::toDuration($settings['maxIdleTime']);

            if ($n1 > 0) {
                $maxIdleTime = $n1;
            }
        }

        $idleCheckInterval = 10;

        if (is_int($settings['idleCheckInterval']) && $settings['idleCheckInterval'] > 0) {
            $idleCheckInterval = $settings['idleCheckInterval'];
        } else if (is_string($settings['idleCheckInterval']) && $settings['idleCheckInterval'] !== '') {
            $n1 = StringUtils::toDuration($settings['idleCheckInterval']);

            if ($n1 > 0) {
                $idleCheckInterval = $n1;
            }
        }

        $this->poolId = $settings['poolType'] . ':' . StringUtils::getRandomString(12);
        $this->minActive = $minActive;
        $this->maxActive = $maxActive;
        $this->takeTimeout = $takeTimeout;
        $this->maxIdleTime = $maxIdleTime;
        $this->idleCheckInterval = $idleCheckInterval;
    }

    private function handleSettings(array $settings): array
    {
        if (!ArrayUtils::isAssocArray($settings)) {
            return [];
        }

        foreach ($settings as $key => $val) {
            $newKey = strtr($key, ['-' => ' ', '_' => ' ']);
            $newKey = preg_replace(Regexp::SPACE_SEP, ' ', $newKey);
            $newKey = str_replace(' ', '', ucwords($newKey));
            $newKey = lcfirst($newKey);

            if ($newKey === $key) {
                continue;
            }

            $settings[$newKey] = $val;
            unset($settings[$key]);
        }

        return $settings;
    }

    private function buildConnectionInternal()
    {
        if (!method_exists($this, 'newConnection')) {
            return null;
        }

        try {
            return $this->newConnection();
        } catch (Throwable $ex) {
            return null;
        }
    }

    private function getCurrentActive(): int
    {
        $table = SwooleTable::getTable(SwooleTable::poolTableName());

        if (!is_object($table)) {
            return 0;
        }

        $key = $this->poolId;
        $data = $table->get($key);
        return is_array($data) ? Cast::toInt($data['currentActive'], 0) : 0;
    }

    private function idleCheckRunning(?bool $flag = null): bool
    {
        $table = SwooleTable::getTable(SwooleTable::poolTableName());

        if (!is_object($table)) {
            return false;
        }

        $key = $this->poolId;

        if (is_bool($flag)) {
            $table->set($key, ['idleCheckRunning' => $flag === true ? 1 : 0]);
            return false;
        }

        $data = $table->get($key);
        return is_array($data) && Cast::toInt($data['idleCheckRunning']) === 1;
    }

    private function markConnFromPool($conn): void
    {
        if (!is_object($conn)) {
            return;
        }

        $table = SwooleTable::getTable(SwooleTable::poolTableName());

        if (!is_object($table)) {
            return;
        }

        $key = 'conn:' . spl_object_hash($conn);

        $table->set($key, [
            'poolId' => $this->poolId,
            'currentActive' => 0,
            'idleCheckRunning' => 0,
            'lastUsedAt' => ''
        ]);
    }

    private function connLastUsedAt($conn, ?int $timestamp = null): int
    {
        if (!is_object($conn)) {
            return 0;
        }

        $table = SwooleTable::getTable(SwooleTable::poolTableName());

        if (!is_object($table)) {
            return 0;
        }

        $key = 'conn:' . spl_object_hash($conn);

        if (is_int($timestamp) && $timestamp > 0) {
            $table->set($key, ['lastUsedAt' => date(DateTimeFormat::FULL, $timestamp)]);
            return 0;
        }

        $data = $table->get($key);
        return is_array($data) && is_string($data['lastUsedAt']) ? strtotime($data['lastUsedAt']) : 0;
    }

    private function logWithRemoveEvent($conn): void
    {
        if (!$this->inDebugMode() || !is_object($conn)) {
            return;
        }

        $logger = $this->logger;

        if (!is_object($logger)) {
            return;
        }

        $workerId = Swoole::getWorkerId();
        $poolType = StringUtils::substringBefore($this->poolId, ':');
        $connectionId = spl_object_hash($conn);

        $msg = sprintf(
            '%s%sconnection[%s] has reach the max idle time, remove from pool',
            $workerId >= 0 ? "in worker$workerId, " : '',
            $poolType,
            $connectionId
        );

        $logger->info($msg);
    }

    private function logTakeSuccess($conn): void
    {
        if (!$this->inDebugMode() || !is_object($conn)) {
            return;
        }

        $logger = $this->logger;

        if (!is_object($logger)) {
            return;
        }

        $workerId = Swoole::getWorkerId();
        $poolType = StringUtils::substringBefore($this->poolId, ':');
        $connectionId = spl_object_hash($conn);

        $msg = sprintf(
            '%ssuccess to take %s connection[%s] from pool',
            $workerId >= 0 ? "in worker$workerId, " : '',
            $poolType,
            $connectionId
        );

        $logger->info($msg);
    }

    private function logTakeFail(): void
    {
        if (!$this->inDebugMode()) {
            return;
        }

        $logger = $this->logger;

        if (!is_object($logger)) {
            return;
        }

        $workerId = Swoole::getWorkerId();
        $poolType = StringUtils::substringBefore($this->poolId, ':');

        $msg = sprintf(
            '%sfail to take %s connection from pool',
            $workerId >= 0 ? "in worker$workerId, " : '',
            $poolType
        );

        $logger->info($msg);
    }

    private function logReleaseSuccess($conn): void
    {
        if (!$this->inDebugMode() || !is_object($conn)) {
            return;
        }

        $logger = $this->logger;

        if (is_object($logger)) {
            return;
        }

        $workerId = Swoole::getWorkerId();
        $poolType = StringUtils::substringBefore($this->poolId, ':');
        $connectionId = spl_object_hash($conn);

        $msg = sprintf(
            '%srelease %s connection[%s] to pool',
            $workerId >= 0 ? "in worker$workerId, " : '',
            $poolType,
            $connectionId
        );

        $logger->info($msg);
    }
}
