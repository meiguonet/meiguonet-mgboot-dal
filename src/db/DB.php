<?php

namespace mgboot\dal\db;

use Closure;
use Illuminate\Support\Collection;
use mgboot\common\AppConf;
use mgboot\common\Cast;
use mgboot\common\constant\Regexp;
use mgboot\common\swoole\Swoole;
use mgboot\common\util\ExceptionUtils;
use mgboot\common\util\FileUtils;
use mgboot\common\util\JsonUtils;
use mgboot\common\util\StringUtils;
use mgboot\dal\ConnectionBuilder;
use mgboot\dal\ConnectionInterface;
use mgboot\dal\GobackendSettings;
use mgboot\dal\pool\PoolInterface;
use mgboot\dal\pool\PoolManager;
use PDO;
use PDOStatement;
use Psr\Log\LoggerInterface;
use Throwable;

final class DB
{
    /**
     * @var array
     */
    private static $map1 = [];

    private function __construct()
    {
    }

    public static function withLogger(LoggerInterface $logger, ?int $workerId = null): void
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "logger_worker$workerId";
        } else {
            $key = 'logger_noworker';
        }

        self::$map1[$key] = $logger;
    }

    private static function getLogger(?int $workerId = null): ?LoggerInterface
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "logger_worker$workerId";
        } else {
            $key = 'logger_noworker';
        }

        $logger = self::$map1[$key];
        return $logger instanceof LoggerInterface ? $logger : null;
    }

    public static function debugLogEnabled(?bool $flag = null, ?int $workerId = null): bool
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "debugLogEnabled_orker$workerId";
        } else {
            $key = 'debugLogEnabled_noworker';
        }

        if (is_bool($flag)) {
            self::$map1[$key] = $flag;
            return false;
        }

        return self::$map1[$key] === true;
    }

    public static function gobackendEnabled(?bool $flag = null, ?int $workerId = null): bool
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "gobackendEnabled_worker$workerId";
        } else {
            $key = 'gobackendEnabled_noworker';
        }

        if (is_bool($flag)) {
            self::$map1[$key] = $flag;
            return false;
        }

        if (self::$map1[$key] !== true) {
            return false;
        }

        $settings = GobackendSettings::loadCurrent($workerId);
        return $settings instanceof GobackendSettings && !$settings->isEnabled();
    }

    public static function withTableSchemasCacheFilepath(string $fpath, ?int $workerId = null): void
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "tableSchemasCacheFilepath_worker$workerId";
        } else {
            $key = 'tableSchemasCacheFilepath_noworker';
        }
        
        self::$map1[$key] = FileUtils::getRealpath($fpath);
    }
    
    private static function getTableSchemasCacheFilepath(?int $workerId = null): string
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "tableSchemasCacheFilepath_worker$workerId";
        } else {
            $key = 'tableSchemasCacheFilepath_noworker';
        }
        
        $fpath = self::$map1[$key];
        
        if (!is_string($fpath) || $fpath === '') {
            $fpath = FileUtils::getRealpath('classpath:cache/table_schemas.php');
        }
        
        return $fpath;
    }

    public static function buildTableSchemas(): void
    {
        if (AppConf::getEnv() === 'dev' && !AppConf::getBoolean('datasource.forceTableSchemasCache')) {
            return;
        }

        if (Swoole::inCoroutineMode(true)) {
            $schemas = self::buildTableSchemasInternal();

            if (empty($schemas)) {
                return;
            }
            
            $workerId = Swoole::getWorkerId();
            $key = "tableSchemas_worker$workerId";
            self::$map1[$key] = $schemas;
            return;
        }

        $cacheFile = self::getTableSchemasCacheFilepath();

        if (is_file($cacheFile)) {
            try {
                $schemas = include($cacheFile);
            } catch (Throwable $ex) {
                $schemas = [];
            }

            if (is_array($schemas) && !empty($schemas)) {
                return;
            }
        }

        self::writeTableSchemasToCacheFile(self::buildTableSchemasInternal());
    }

    public static function getTableSchemas(): array
    {
        if (AppConf::getEnv() === 'dev' && !AppConf::getBoolean('datasource.forceTableSchemasCache')) {
            return self::buildTableSchemasInternal();
        }

        if (Swoole::inCoroutineMode(true)) {
            $workerId = Swoole::getWorkerId();
            $key = "tableSchemas_worker$workerId";
            $schemas = self::$map1[$key];

            if (!is_array($schemas) || empty($schemas)) {
                self::buildTableSchemas();
                $schemas = self::$map1[$key];
            }

            return is_array($schemas) ? $schemas : [];
        }

        $cacheFile = self::getTableSchemasCacheFilepath();

        if (!is_file($cacheFile)) {
            self::buildTableSchemas();
        }

        if (!is_file($cacheFile)) {
            return [];
        }

        try {
            $schemas = include($cacheFile);
        } catch (Throwable $ex) {
            $schemas = [];
        }

        return is_array($schemas) ? $schemas : [];
    }

    public static function getTableSchema(string $tableName): array
    {
        $tableName = str_replace('`', '', $tableName);

        if (strpos($tableName, '.') !== false) {
            $tableName = StringUtils::substringAfterLast($tableName, '.');
        }

        $schemas = self::getTableSchemas();
        return $schemas[$tableName] ?? [];
    }

    public static function table(string $tableName): QueryBuilder
    {
        return QueryBuilder::create($tableName);
    }

    public static function raw(string $expr): Expression
    {
        return Expression::create($expr);
    }

    public static function selectBySql(string $sql, array $params = [], ?TxManager $txm = null): Collection
    {
        $logger = self::getLogger();
        $canWriteLog = self::debugLogEnabled() && $logger instanceof LoggerInterface;

        if (self::gobackendEnabled()) {
            if ($canWriteLog) {
                $logger->info('DB Context run in gobackend mode');
            }

            self::logSql($sql, $params);
            list($result, $errorTips) = self::fromGobackend('@@select', $sql, $params);

            if (!empty($errorTips)) {
                $ex = new DbException(null, $errorTips);
                self::writeErrorLog($ex);
                throw $ex;
            }

            return collect(JsonUtils::arrayFrom($result));
        }

        try {
            /* @var PDO $pdo */
            list($fromTxManager, $pdo) = self::getPdoConnection($txm);

            if ($fromTxManager && $canWriteLog) {
                $logger->info('DB Context run in transation mode');
            } else if ($pdo instanceof ConnectionInterface && $canWriteLog) {
                $logger->info('DB Context run in connection pool mode');
            }

            self::logSql($sql, $params);
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $err = null;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return collect([]);
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
            return collect($stmt->fetchAll());
        } catch (Throwable $ex) {
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            if (!$fromTxManager) {
                PoolManager::releaseConnection($pdo, $err);
            }
        }
    }

    public static function firstBySql(string $sql, array $params = [], ?TxManager $txm = null): ?array
    {
        $logger = self::getLogger();
        $canWriteLog = self::debugLogEnabled() && $logger instanceof LoggerInterface;

        if (self::gobackendEnabled()) {
            if ($canWriteLog) {
                $logger->info('DB Context run in gobackend mode');
            }

            self::logSql($sql, $params);
            list($result, $errorTips) = self::fromGobackend('@@first', $sql, $params);

            if (!empty($errorTips)) {
                $ex = new DbException(null, $errorTips);
                self::writeErrorLog($ex);
                throw $ex;
            }

            $map1 = JsonUtils::mapFrom($result);
            return is_array($map1) ? $map1 : null;
        }

        try {
            /* @var PDO $pdo */
            list($fromTxManager, $pdo) = self::getPdoConnection($txm);

            if ($fromTxManager && $canWriteLog) {
                $logger->info('DB Context run in transation mode');
            } else if ($pdo instanceof ConnectionInterface && $canWriteLog) {
                $logger->info('DB Context run in connection pool mode');
            }

            self::logSql($sql, $params);
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $err = null;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return null;
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
            $data = $stmt->fetch();
            return is_array($data) ? $data : null;
        } catch (Throwable $ex) {
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            if (!$fromTxManager) {
                PoolManager::releaseConnection($pdo, $err);
            }
        }
    }

    public static function countBySql(string $sql, array $params = [], ?TxManager $txm = null): int
    {
        $logger = self::getLogger();
        $canWriteLog = self::debugLogEnabled() && $logger instanceof LoggerInterface;

        if (self::gobackendEnabled()) {
            if ($canWriteLog) {
                $logger->info('DB Context run in gobackend mode');
            }

            self::logSql($sql, $params);
            list($result, $errorTips) = self::fromGobackend('@@count', $sql, $params);

            if (!empty($errorTips)) {
                $ex = new DbException(null, $errorTips);
                self::writeErrorLog($ex);
                throw $ex;
            }

            return Cast::toInt($result, 0);
        }

        try {
            /* @var PDO $pdo */
            list($fromTxManager, $pdo) = self::getPdoConnection($txm);

            if ($fromTxManager && $canWriteLog) {
                $logger->info('DB Context run in transation mode');
            } else if ($pdo instanceof ConnectionInterface && $canWriteLog) {
                $logger->info('DB Context run in connection pool mode');
            }

            self::logSql($sql, $params);
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $err = null;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
            return $stmt->fetchColumn();
        } catch (Throwable $ex) {
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            if (!$fromTxManager) {
                PoolManager::releaseConnection($pdo, $err);
            }
        }
    }

    public static function insertBySql(string $sql, array $params = [], ?TxManager $txm = null): int
    {
        $logger = self::getLogger();
        $canWriteLog = self::debugLogEnabled() && $logger instanceof LoggerInterface;

        if (self::gobackendEnabled()) {
            if ($canWriteLog) {
                $logger->info('DB Context run in gobackend mode');
            }

            self::logSql($sql, $params);
            list($result, $errorTips) = self::fromGobackend('@@insert', $sql, $params);

            if (!empty($errorTips)) {
                $ex = new DbException(null, $errorTips);
                self::writeErrorLog($ex);
                throw $ex;
            }

            return Cast::toInt($result, 0);
        }

        try {
            /* @var PDO $pdo */
            list($fromTxManager, $pdo) = self::getPdoConnection($txm);

            if ($fromTxManager && $canWriteLog) {
                $logger->info('DB Context run in transation mode');
            } else if ($pdo instanceof ConnectionInterface && $canWriteLog) {
                $logger->info('DB Context run in connection pool mode');
            }

            self::logSql($sql, $params);
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $err = null;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);

            if (!$stmt->execute()) {
                return 0;
            }

            return (int) $pdo->lastInsertId();
        } catch (Throwable $ex) {
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            if (!$fromTxManager) {
                PoolManager::releaseConnection($pdo, $err);
            }
        }
    }

    public static function updateBySql(string $sql, array $params = [], ?TxManager $txm = null): int
    {
        $logger = self::getLogger();
        $canWriteLog = self::debugLogEnabled() && $logger instanceof LoggerInterface;

        if (self::gobackendEnabled()) {
            if ($canWriteLog) {
                $logger->info('DB Context run in gobackend mode');
            }

            self::logSql($sql, $params);
            list($result, $errorTips) = self::fromGobackend('@@update', $sql, $params);

            if (!empty($errorTips)) {
                $ex = new DbException(null, $errorTips);
                self::writeErrorLog($ex);
                throw $ex;
            }

            return Cast::toInt($result, -1);
        }

        try {
            /* @var PDO $pdo */
            list($fromTxManager, $pdo) = self::getPdoConnection($txm);

            if ($fromTxManager && $canWriteLog) {
                $logger->info('DB Context run in transation mode');
            } else if ($pdo instanceof ConnectionInterface && $canWriteLog) {
                $logger->info('DB Context run in connection pool mode');
            }

            self::logSql($sql, $params);
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $err = null;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);

            if (!$stmt->execute()) {
                return 0;
            }

            return $stmt->rowCount();
        } catch (Throwable $ex) {
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            if (!$fromTxManager) {
                PoolManager::releaseConnection($pdo, $err);
            }
        }
    }

    /**
     * @param string $sql
     * @param array $params
     * @param TxManager|null $txm
     * @return int|float|string
     */
    public static function sumBySql(string $sql, array $params = [], ?TxManager $txm = null)
    {
        $logger = self::getLogger();
        $canWriteLog = self::debugLogEnabled() && $logger instanceof LoggerInterface;

        if (self::gobackendEnabled()) {
            if ($canWriteLog) {
                $logger->info('DB Context run in gobackend mode');
            }

            self::logSql($sql, $params);
            list($result, $errorTips) = self::fromGobackend('@@sum', $sql, $params);

            if (!empty($errorTips)) {
                $ex = new DbException(null, $errorTips);
                self::writeErrorLog($ex);
                throw $ex;
            }

            $map1 = JsonUtils::mapFrom($result);

            if (!is_array($map1)) {
                return '0.00';
            }

            $num = $map1['sum'];
            return is_int($num) || is_float($num) ? $num : bcadd($num, 0, 2);
        }

        try {
            /* @var PDO $pdo */
            list($fromTxManager, $pdo) = self::getPdoConnection($txm);

            if ($fromTxManager && $canWriteLog) {
                $logger->info('DB Context run in transation mode');
            } else if ($pdo instanceof ConnectionInterface && $canWriteLog) {
                $logger->info('DB Context run in connection pool mode');
            }

            self::logSql($sql, $params);
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $err = null;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);

            if (!$stmt->execute()) {
                return 0;
            }

            $value = $stmt->fetchColumn();

            if (is_int($value) || is_float($value)) {
                return $value;
            }

            if (!is_string($value) || $value === '') {
                return 0;
            }

            if (StringUtils::isInt($value)) {
                return Cast::toInt($value);
            }

            if (StringUtils::isFloat($value)) {
                return bcadd($value, 0, 2);
            }

            return 0;
        } catch (Throwable $ex) {
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            if (!$fromTxManager) {
                PoolManager::releaseConnection($pdo, $err);
            }
        }
    }

    public static function deleteBySql(string $sql, array $params = [], ?TxManager $txm = null): int
    {
        return self::updateBySql($sql, $params, $txm);
    }

    public static function executeSql(string $sql, array $params = [], ?TxManager $txm = null): void
    {
        $logger = self::getLogger();
        $canWriteLog = self::debugLogEnabled() && $logger instanceof LoggerInterface;

        if (self::gobackendEnabled()) {
            if ($canWriteLog) {
                $logger->info('DB Context run in gobackend mode');
            }

            self::logSql($sql, $params);
            list(, $errorTips) = self::fromGobackend('@@execute', $sql, $params);

            if (!empty($errorTips)) {
                $ex = new DbException(null, $errorTips);
                self::writeErrorLog($ex);
                throw $ex;
            }

            return;
        }

        try {
            /* @var PDO $pdo */
            list($fromTxManager, $pdo) = self::getPdoConnection($txm);

            if ($fromTxManager && $canWriteLog) {
                $logger->info('DB Context run in transation mode');
            } else if ($pdo instanceof ConnectionInterface && $canWriteLog) {
                $logger->info('DB Context run in connection pool mode');
            }

            self::logSql($sql, $params);
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $err = null;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return;
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
        } catch (Throwable $ex) {
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            if (!$fromTxManager) {
                PoolManager::releaseConnection($pdo, $err);
            }
        }
    }

    private static function fromGobackend(string $cmd, string $query, array $params): array
    {
        $cfg = GobackendSettings::loadCurrent();

        if (!$cfg->isEnabled()) {
            return ['', 'fail to load gobackend settings'];
        }

        $host = $cfg->getHost();
        $port = $cfg->getPort();

        switch ($cmd) {
            case '@@select':
                $timeout = 10;
                break;
            case '@@first':
            case '@@count':
            case '@@sum':
                $timeout = 5;
                break;
            default:
                $timeout = 2;
        }

        switch ($cmd) {
            case '@@select':
                $maxPkgLength = 8 * 1024 * 1024;
                break;
            case '@@first':
                $maxPkgLength = 16 * 1024;
                break;
            default:
                $maxPkgLength = 256;
                break;
        }

        $msg = "@@db:$cmd:$query";

        if (!empty($params)) {
            $msg .= '@^sep^@' . JsonUtils::toJson($params);
        }

        if (Swoole::inCoroutineMode(true)) {
            return self::fromGobackendAsync([$host, $port, $timeout, $maxPkgLength, $msg]);
        }

        $fp = fsockopen($host, $port);

        if (!is_resource($fp)) {
            $errorTips = 'fail to connect to gobackend';
            return ['', $errorTips];
        }

        try {
            fwrite($fp, $msg);
            stream_set_timeout($fp, $timeout);
            $result = '';

            while (!feof($fp)) {
                $buf = fread($fp, $maxPkgLength);
                $info = stream_get_meta_data($fp);

                if ($info['timed_out']) {
                    return ['', 'read timeout'];
                }

                if (!is_string($buf)) {
                    continue;
                }

                $result .= $buf;
            }

            if (!is_string($result) || $result === '') {
                return ['', 'no contents readed'];
            }

            $result = trim(str_replace('@^@end', '', $result));

            if (StringUtils::startsWith($result, '@@error:')) {
                return ['', str_replace('@@error:', '', $result)];
            }

            return [$result, ''];
        } catch (Throwable $ex) {
            return ['', $ex->getMessage()];
        } finally {
            fclose($fp);
        }
    }

    private static function fromGobackendAsync(array $payloads): array
    {
        $ret = ['', ''];
        list ($host, $port, $timeout, $maxPkgLength, $msg) = $payloads;
        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        $socket = new \Swoole\Coroutine\Socket(AF_INET, SOCK_STREAM, 0);

        if ($socket->connect($host, $port, 0.5) !== true) {
            $ret[1] = 'fail to connect to gobackend';
            return $ret;
        }

        $n1 = $socket->sendAll($msg);

        if (!is_int($n1) || $n1 < strlen($msg)) {
            $socket->close();
            $ret[1] = 'fail to send data to gobackend';
            return $ret;
        }

        $result = $socket->recvAll($maxPkgLength, floatval($timeout));
        $socket->close();

        if (is_string($result)) {
            $result = trim(str_replace('@^@end', '', $result));

            if (StringUtils::startsWith($result, '@@error:')) {
                $ret[1] = str_replace('@@error:', '', $result);
            } else {
                $ret[0] = $result;
            }
        } else {
            $ret[1] = 'fail to read data from gobackend';
        }

        return $ret;
    }

    /**
     * @param Closure $callback
     * @param int|string|null $timeout
     */
    public static function transations(Closure $callback, $timeout = null): void
    {
        if (Swoole::inCoroutineMode(true)) {
            if (is_string($timeout) && $timeout !== '') {
                $timeout = Cast::toDuration($timeout);
            }

            if (!is_int($timeout) || $timeout < 1) {
                $timeout = 30;
            }

            self::transationsAsync($callback, $timeout);
            return;
        }

        $pdo = ConnectionBuilder::buildPdoConnection();

        if (!is_object($pdo)) {
            throw new DbException(null, 'fail to get database connection');
        }

        $txm = TxManager::create($pdo);
        $err = null;

        try {
            $pdo->beginTransaction();
            $callback->call($txm);
            $pdo->commit();
        } catch (Throwable $ex) {
            $pdo->rollBack();
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            PoolManager::releaseConnection($pdo, $err);
        }
    }

    private static function transationsAsync(Closure $callback, int $timeout): void
    {
        $pdo = null;
        $pool = PoolManager::getPool('pdo');

        if ($pool instanceof PoolInterface) {
            try {
                $pdo = $pool->take();
            } catch (Throwable $ex) {
                $pdo = null;
            }
        }

        if (!is_object($pdo)) {
            $pdo = ConnectionBuilder::buildPdoConnection();
        }

        if (!is_object($pdo) || !($pdo instanceof PDO)) {
            throw new DbException(null, 'fail to get database connection');
        }

        $txm = TxManager::create($pdo);
        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        $wg = new \Swoole\Coroutine\WaitGroup();
        $wg->add();

        try {
            go(function () use ($callback, $pdo, $txm, $wg) {
                $err = null;

                try {
                    $pdo->beginTransaction();
                    $callback->call($txm);
                    $pdo->commit();
                } catch (Throwable $ex) {
                    $pdo->rollBack();
                    $err = self::wrapAsDbException($ex);
                    self::writeErrorLog($err);
                    throw $err;
                } finally {
                    PoolManager::releaseConnection($pdo, $err);
                    $wg->done();
                }
            });

            $wg->wait(floatval($timeout));
        } catch (Throwable $ex) {
            throw new DbException(null, $ex->getMessage());
        }
    }

    private static function getPdoConnection(?TxManager $txm): array
    {
        if (is_object($txm)) {
            return [true, $txm->getPdo()];
        }

        $ex = new DbException(null, "fail to get database connection");
        $pdo = PoolManager::getConnection('pdo');

        if (!is_object($pdo) || !($pdo instanceof PDO)) {
            throw $ex;
        }

        return [false, $pdo];
    }

    private static function pdoBindParams(PDOStatement $stmt, array $params): void
    {
        if (empty($params)) {
            return;
        }

        foreach ($params as $i => $value) {
            if ($value === null) {
                $stmt->bindValue($i + 1, null, PDO::PARAM_NULL);
                continue;
            }

            if (is_int($value)) {
                $stmt->bindValue($i + 1, $value, PDO::PARAM_INT);
                continue;
            }

            if (is_float($value)) {
                $stmt->bindValue($i + 1, "$value");
                continue;
            }

            if (is_string($value)) {
                $stmt->bindValue($i + 1, $value);
                continue;
            }

            if (is_bool($value)) {
                $stmt->bindValue($i + 1, $value, PDO::PARAM_BOOL);
                continue;
            }

            if (is_array($value)) {
                throw new DbException(null, 'fail to bind param, param type: array');
            }

            if (is_resource($value)) {
                throw new DbException(null, 'fail to bind param, param type: resource');
            }

            if (is_object($value)) {
                throw new DbException(null, 'fail to bind param, param type: ' . get_class($value));
            }
        }
    }

    private static function buildTableSchemasInternal(): array
    {
        $pdo = ConnectionBuilder::buildPdoConnection();

        if (!is_object($pdo)) {
            return [];
        }

        $tables = [];

        try {
            $stmt = $pdo->prepare('SHOW TABLES');
            $stmt->execute();
            $records = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (!is_array($records) || empty($records)) {
                unset($pdo);
                return [];
            }

            foreach ($records as $record) {
                foreach ($record as $key => $value) {
                    if (strpos($key, 'Tables_in') !== false) {
                        $tables[] = trim($value);
                        break;
                    }
                }
            }
        } catch (Throwable $ex) {
            unset($pdo);
            return [];
        }

        if (empty($tables)) {
            unset($pdo);
            return [];
        }

        $schemas = [];

        foreach ($tables as $tableName) {
            try {
                $stmt = $pdo->prepare("DESC $tableName");
                $stmt->execute();
                $items = $stmt->fetchAll(PDO::FETCH_ASSOC);

                if (!is_array($items) || empty($items)) {
                    continue;
                }

                $fieldNames = [
                    'ctime',
                    'create_at',
                    'createAt',
                    'create_time',
                    'createTime',
                    'update_at',
                    'updateAt',
                    'delete_at',
                    'deleteAt',
                    'del_flag',
                    'delFlag'
                ];

                $schema = [];

                foreach ($items as $item) {
                    $fieldName = $item['Field'];

                    if (!in_array($fieldName, $fieldNames)) {
                        continue;
                    }

                    $nullable = stripos($item['Null'], 'YES') !== false;
                    $isPrimaryKey = $item['Key'] === 'PRI';
                    $defaultValue = $item['Default'];
                    $autoIncrement = $item['Extra'] === 'auto_increment';
                    $parts = preg_split(Regexp::SPACE_SEP, $item['Type']);

                    if (strpos($parts[0], '(') !== false) {
                        $fieldType = StringUtils::substringBefore($parts[0], '(');
                        $fieldSize = str_replace($fieldType, '', $parts[0]);
                    } else {
                        $fieldType = $parts[0];
                        $fieldSize = '';
                    }

                    if (!StringUtils::startsWith($fieldSize, '(') || !StringUtils::endsWith($fieldSize, ')')) {
                        $fieldSize = '';
                    } else {
                        $fieldSize = rtrim(ltrim($fieldSize, '('), ')');
                    }

                    if (is_numeric($fieldSize)) {
                        $fieldSize = (int) $fieldSize;
                    }

                    $unsigned = stripos($item['Type'], 'unsigned') !== false;

                    $schema[] = compact(
                        'fieldName',
                        'fieldType',
                        'fieldSize',
                        'unsigned',
                        'nullable',
                        'defaultValue',
                        'autoIncrement',
                        'isPrimaryKey'
                    );
                }
            } catch (Throwable $ex) {
                $schema = null;
            }

            if (!is_array($schema) || empty($schema)) {
                continue;
            }

            $schemas[$tableName] = $schema;
        }

        unset($pdo);
        return $schemas;
    }

    private static function writeTableSchemasToCacheFile(array $schemas): void
    {
        if (empty($schemas)) {
            return;
        }

        $cacheFile = self::getTableSchemasCacheFilepath();
        $dir = dirname($cacheFile);
        
        if (!is_string($dir) || $dir === '') {
            return;
        }
        
        if (!is_dir($dir)) {
            mkdir($dir, 0644, true);
        }
        
        if (!is_dir($dir) || !is_writable($dir)) {
            return;
        }
        
        $cacheFile = FileUtils::getRealpath($cacheFile);
        $fp = fopen($cacheFile, 'w');

        if (!is_resource($fp)) {
            return;
        }

        $sb = [
            "<?php\n",
            'return ' . var_export($schemas, true) . ";\n"
        ];

        flock($fp, LOCK_EX);
        fwrite($fp, implode('', $sb));
        flock($fp, LOCK_UN);
        fclose($fp);
    }

    private static function wrapAsDbException(Throwable $ex): DbException
    {
        if ($ex instanceof DbException) {
            return $ex;
        }

        return new DbException(null, $ex->getMessage());
    }

    private static function logSql(string $sql, ?array $params = null): void
    {
        $logger = self::getLogger();

        if (!is_object($logger) || !self::debugLogEnabled()) {
            return;
        }

        $logger->info($sql);

        if (is_array($params) && !empty($params)) {
            $logger->debug('params: ' . JsonUtils::toJson($params));
        }
    }

    /**
     * @param string|Throwable $msg
     */
    private static function writeErrorLog($msg): void
    {
        $logger = self::getLogger();

        if (!is_object($logger)) {
            return;
        }

        if ($msg instanceof Throwable) {
            $msg = ExceptionUtils::getStackTrace($msg);
        }

        $logger->error($msg);
    }
}
