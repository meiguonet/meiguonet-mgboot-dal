<?php

namespace mgboot\dal\pool;

use mgboot\common\Cast;
use mgboot\common\constant\Regexp;
use mgboot\common\util\ArrayUtils;
use mgboot\common\util\StringUtils;
use mgboot\dal\ConnectionBuilder;
use mgboot\dal\ConnectionInterface;
use Psr\Log\LoggerInterface;
use Redis;
use RuntimeException;
use Throwable;

trait PoolTrait
{
    /**
     * @var int
     */
    private $workerId;

    /**
     * @var string
     */
    private $poolId;

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
     * @var PoolInfo
     */
    private $poolInfo;

    /**
     * @var bool
     */
    private $isInDebugMode = false;

    /**
     * @var LoggerInterface|null
     */
    private $logger = null;

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

    public function getPoolType(): string
    {
        return StringUtils::substringBefore($this->poolId, ':');
    }

    public function run(): void
    {
        $currentActive = 0;

        for ($i = 1; $i <= $this->minActive; $i++) {
            $conn = $this->buildConnectionInternal();

            if (!is_object($conn)) {
                continue;
            }

            $currentActive++;
            $this->poolInfo->getConnChan()->push($conn);
        }

        $this->updateCurrentActive($currentActive);
        $logger = $this->logger;

        if (is_object($logger) && $this->inDebugMode()) {
            $msg = sprintf(
                'in worker%d, %s pool[minActive=%d, maxActive=%d, currentActive=%d, takeTimeout=%ds, maxIdleTime=%ds, idleCheckInterval=%ds] is running',
                $this->workerId,
                $this->getPoolType(),
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
        $poolType = $this->getPoolType();
        $conn = null;
        $ex1 = new RuntimeException("fail to take $poolType connection from connection pool");

        if ($this->closed() || $this->idleCheckRunning()) {
            switch ($poolType) {
                case 'pdo':
                case 'db':
                    $conn = ConnectionBuilder::buildPdoConnection();
                    break;
                case 'redis':
                    $conn = ConnectionBuilder::buildRedisConnection();
                    break;
            }

            if (!is_object($conn)) {
                throw $ex1;
            }

            return $conn;
        }

        $conn = $this->poolInfo->getConnChan()->pop(0.01);

        if (is_object($conn)) {
            $this->connLastUsedAt($conn, time());
            $this->logTakeSuccess($conn);
            return $conn;
        }

        if ($this->getCurrentActive() < $this->maxActive) {
            $conn = $this->buildConnectionInternal();

            if (is_object($conn)) {
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

        $conn = $this->poolInfo->getConnChan()->pop($timeout);

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
        if (!is_object($conn) || !($conn instanceof ConnectionInterface)) {
            return;
        }

        if ($conn->getPoolId() !== $this->poolId) {
            return;
        }

        $this->poolInfo->getConnChan()->push($conn);
        $this->logReleaseSuccess($conn);
    }

    public function updateCurrentActive(int $num): void
    {
        if ($num === 1) {
            $this->poolInfo->getCurrentActiveAtomic()->add(1);
            return;
        }

        if ($num === -1) {
            if ($this->getCurrentActive() !== 0) {
                $this->poolInfo->getCurrentActiveAtomic()->sub(1);
            }

            return;
        }

        $this->poolInfo->getCurrentActiveAtomic()->set($num);
    }

    public function destroy($timeout = null): void
    {
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

            for ($i = 1; $i <= $this->poolInfo->getMaxActive(); $i++) {
                $conn = $this->poolInfo->getConnChan()->pop(0.01);

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

        $this->poolInfo->getConnChan()->close();
    }

    private function init(int $workerId, PoolInfo $poolInfo, array $settings): void
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

        $poolInfo->setMaxActive($maxActive);
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

        $this->workerId = $workerId;
        $this->poolId = $settings['poolType'] . ':' . StringUtils::getRandomString(12);
        $this->minActive = $minActive;
        $this->maxActive = $maxActive;
        $this->takeTimeout = $takeTimeout;
        $this->maxIdleTime = $maxIdleTime;
        $this->idleCheckInterval = $idleCheckInterval;
        $this->poolInfo = $poolInfo;
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

    private function runIdleChecker(): void
    {
        $ch = $this->poolInfo->getConnChan();

        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        \Swoole\Timer::tick($this->idleCheckInterval * 1000, function () use ($ch) {
            $this->idleCheckRunning(true);
            $this->logIdleCheckStart();
            $now = time();
            $connections = [];

            while (true) {
                if (time() - $now > 15) {
                    break;
                }

                for ($i = 1; $i <= $this->maxActive; $i++) {
                    $conn = $ch->pop(0.01);

                    if (!is_object($conn) || !($conn instanceof ConnectionInterface)) {
                        continue;
                    }

                    $lastUsedAt = $conn->getLastUsedAt();

                    if (!is_int($lastUsedAt) || $lastUsedAt < 1) {
                        $conn->updateLastUsedAt(time());
                        $connections[] = $conn;
                        continue;
                    }

                    if ($now - $lastUsedAt >= $this->maxIdleTime) {
                        $this->updateCurrentActive(-1);
                        $this->logWithRemoveEvent($conn);

                        if ($conn instanceof Redis) {
                            $conn->close();
                        }

                        unset($conn);
                        continue;
                    }

                    $connections[] = $conn;
                }

                /** @noinspection PhpFullyQualifiedNameUsageInspection */
                \Swoole\Coroutine::sleep(0.05);
            }

            foreach ($connections as $conn) {
                $ch->push($conn);
            }

            $this->idleCheckRunning(false);
        });
    }

    private function buildConnectionInternal(): ?ConnectionInterface
    {
        if (!method_exists($this, 'newConnection')) {
            return null;
        }

        try {
            $conn = $this->newConnection();
        } catch (Throwable $ex) {
            $conn = null;
        }

        return $conn instanceof ConnectionInterface ? $conn : null;
    }

    private function getCurrentActive(): int
    {
        $n1 = $this->poolInfo->getCurrentActiveAtomic()->get();
        return is_int($n1) ? $n1 : 0;
    }

    private function getIdleCount(): int
    {
        $data = $this->poolInfo->getConnChan()->stats();
        return is_array($data) ? Cast::toInt($data['queue_num'], 0) : 0;
    }

    private function idleCheckRunning(?bool $flag = null): bool
    {
        if (is_bool($flag)) {
            if ($flag) {
                $this->poolInfo->getIdleCheckRunningAtomic()->cmpset(0, 1);
            } else {
                $this->poolInfo->getIdleCheckRunningAtomic()->cmpset(1, 0);
            }

            return false;
        }

        return $this->poolInfo->getIdleCheckRunningAtomic()->get() === 1;
    }

    private function closed(?bool $flag = null): bool
    {
        if (is_bool($flag)) {
            if ($flag) {
                $this->poolInfo->getClosedAtomic()->cmpset(0, 1);
            } else {
                $this->poolInfo->getClosedAtomic()->cmpset(1, 0);
            }

            return false;
        }

        return $this->poolInfo->getClosedAtomic()->get() === 1;
    }

    private function connLastUsedAt($conn, ?int $timestamp = null): int
    {
        if (!is_object($conn) || !($conn instanceof ConnectionInterface)) {
            return 0;
        }

        if (is_int($timestamp) && $timestamp > 0) {
            $conn->updateLastUsedAt($timestamp);
            return 0;
        }

        $ts = $conn->getLastUsedAt();
        return is_int($ts) ? $ts : 0;
    }

    private function logIdleCheckStart(): void
    {
        $logger = $this->logger;

        if (!is_object($logger) || !$this->inDebugMode()) {
            return;
        }

        $workerId = $this->workerId;

        $msg = sprintf(
            '%s%s pool start idle connection check...',
            $workerId >= 0 ? "in worker$workerId, " : '',
            $this->getPoolType()
        );

        $logger->info($msg);
    }

    private function logWithRemoveEvent($conn): void
    {
        if (!$this->inDebugMode() || !is_object($conn) || !($conn instanceof ConnectionInterface)) {
            return;
        }

        $logger = $this->logger;

        if (!is_object($logger)) {
            return;
        }

        $workerId = $this->workerId;

        $msg = sprintf(
            '%s%s connection[%s] has reach the max idle time, remove from pool, pool stats: [currentActive=%d, idleCount=%d]',
            $workerId >= 0 ? "in worker$workerId, " : '',
            $this->getPoolType(),
            $conn->getConnectionId(),
            $this->getCurrentActive(),
            $this->getIdleCount()
        );

        $logger->info($msg);
    }

    private function logTakeSuccess($conn): void
    {
        if (!$this->inDebugMode() || !is_object($conn) || !($conn instanceof ConnectionInterface)) {
            return;
        }

        $logger = $this->logger;

        if (!is_object($logger)) {
            return;
        }

        $workerId = $this->workerId;

        $msg = sprintf(
            '%ssuccess to take %s connection[%s] from pool, pool stats: [currentActive=%d, idleCount=%d]',
            $workerId >= 0 ? "in worker$workerId, " : '',
            $this->getPoolType(),
            $conn->getConnectionId(),
            $this->getCurrentActive(),
            $this->getIdleCount()
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

        $workerId = $this->workerId;

        $msg = sprintf(
            '%sfail to take %s connection from pool',
            $workerId >= 0 ? "in worker$workerId, " : '',
            $this->getPoolType()
        );

        $logger->info($msg);
    }

    private function logReleaseSuccess($conn): void
    {
        if (!$this->inDebugMode() || !is_object($conn) || !($conn instanceof ConnectionInterface)) {
            return;
        }

        $logger = $this->logger;

        if (!is_object($logger)) {
            return;
        }

        $workerId = $this->workerId;

        $msg = sprintf(
            '%srelease %s connection[%s] to pool, pool stats: [currentActive=%d, idleCount=%d]',
            $workerId >= 0 ? "in worker$workerId, " : '',
            $this->getPoolType(),
            $conn->getConnectionId(),
            $this->getCurrentActive(),
            $this->getIdleCount()
        );

        $logger->info($msg);
    }
}
