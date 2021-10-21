<?php

namespace mgboot\dal\db;

use Closure;
use Illuminate\Support\Collection;
use mgboot\common\AppConf;
use mgboot\common\Cast;
use mgboot\common\swoole\SwooleTable;
use mgboot\common\util\ArrayUtils;
use mgboot\common\constant\Regexp;
use mgboot\dal\ConnectionBuilder;
use mgboot\dal\GobackendSettings;
use mgboot\common\swoole\Swoole;
use mgboot\common\util\ExceptionUtils;
use mgboot\common\util\FileUtils;
use mgboot\common\util\JsonUtils;
use mgboot\common\util\StringUtils;
use mgboot\dal\pool\PoolInterface;
use mgboot\dal\pool\PoolManager;
use PDO;
use PDOStatement;
use Psr\Log\LoggerInterface;
use Throwable;

final class DB
{
    /**
     * @var LoggerInterface|null
     */
    private static $logger = null;

    /**
     * @var bool
     */
    private static $_debugLogEnabled = false;

    /**
     * @var DbConfig|null
     */
    private static $dbConfig = null;

    /**
     * @var GobackendSettings|null
     */
    private static $gobackendSettings = null;

    /**
     * @var string
     */
    private static $cacheDir = 'classpath:cache';

    /**
     * @var string
     */
    private static $cacheKeyTableSchemas = 'tableSchemas';

    private function __construct()
    {
    }

    public static function withLogger(LoggerInterface $logger): void
    {
        self::$logger = $logger;
    }

    public static function debugLogEnabled(?bool $flag = null): bool
    {
        if (is_bool($flag)) {
            self::$_debugLogEnabled = $flag === true;
            return false;
        }

        return self::$_debugLogEnabled;
    }

    public static function withDbConfig(array $settings): void
    {
        self::$dbConfig = DbConfig::create($settings);
    }

    public static function getDbConfig(): DbConfig
    {
        $cfg = self::$dbConfig;
        return $cfg instanceof DbConfig ? $cfg : DbConfig::create();
    }

    public static function gobackendEnabled(?array $settings = null): bool
    {
        if (ArrayUtils::isAssocArray($settings)) {
            self::$gobackendSettings = GobackendSettings::create($settings);
            return false;
        }

        $settings = self::$gobackendSettings;
        return $settings instanceof GobackendSettings && $settings->isEnabled();
    }

    public static function getGobackendSettings(): GobackendSettings
    {
        $settings = self::$gobackendSettings;
        return $settings instanceof GobackendSettings ? $settings : GobackendSettings::create();
    }

    public static function withCacheDir(string $dir): void
    {
        if ($dir !== '' && is_dir($dir) && is_writable($dir)) {
            self::$cacheDir = $dir;
        }
    }

    public static function buildTableSchemas(): void
    {
        if (AppConf::getEnv() === 'dev' && !AppConf::getBoolean('datasource.forceTableSchemasCache')) {
            return;
        }

        if (is_object(Swoole::getServer())) {
            $schemas = self::buildTableSchemasInternal();

            if (empty($schemas)) {
                return;
            }

            $table = SwooleTable::getTable(SwooleTable::cacheTableName());

            if (!is_object($table)) {
                return;
            }

            $table->set(self::$cacheKeyTableSchemas, [
                'value' => JsonUtils::toJson($schemas),
                'expiry' => 0
            ]);

            return;
        }

        $cacheFile = rtrim(self::$cacheDir, '/') . '/table_schemas.php';
        $cacheFile = FileUtils::getRealpath($cacheFile);

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

        if (is_object(Swoole::getServer())) {
            $table = SwooleTable::getTable(SwooleTable::cacheTableName());

            if (!is_object($table)) {
                return [];
            }

            $entry = $table->get(self::$cacheKeyTableSchemas);

            if (!is_array($entry) || !is_string($entry['value'])) {
                return [];
            }

            $schemas = JsonUtils::mapFrom($entry['value']);
            return is_array($schemas) ? $schemas : [];
        }

        $cacheFile = rtrim(self::$cacheDir, '/') . '/table_schemas.php';
        $cacheFile = FileUtils::getRealpath($cacheFile);

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
        $logger = self::$logger;
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

            if (PoolManager::isFromPool($pdo) && $canWriteLog) {
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
        $logger = self::$logger;
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

            if (PoolManager::isFromPool($pdo) && $canWriteLog) {
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
        $logger = self::$logger;
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

            if (PoolManager::isFromPool($pdo) && $canWriteLog) {
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
        $logger = self::$logger;
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

            if (PoolManager::isFromPool($pdo) && $canWriteLog) {
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
        $logger = self::$logger;
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

            if (PoolManager::isFromPool($pdo) && $canWriteLog) {
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
        $logger = self::$logger;
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

            if (PoolManager::isFromPool($pdo) && $canWriteLog) {
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
        $logger = self::$logger;
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

            if (PoolManager::isFromPool($pdo) && $canWriteLog) {
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
        $cfg = self::getGobackendSettings();

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

    public static function transations(Closure $callback): void
    {
        if (Swoole::inCoroutineMode(true)) {
            self::transationsAsync($callback);
            return;
        }

        $pdo = ConnectionBuilder::buildPdoConnection();

        if ($pdo === null) {
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

    private static function transationsAsync(Closure $callback): void
    {
        $pool = PoolManager::getPool('pdo');

        if (!($pool instanceof PoolInterface)) {
            throw new DbException(null, 'fail to get database connection');
        }

        try {
            $pdo = $pool->take();
        } catch (Throwable $ex) {
            throw new DbException(null, $ex->getMessage());
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

            $wg->wait();
        } catch (Throwable $ex) {
            throw new DbException(null, $ex->getMessage());
        }
    }

    private static function getPdoConnection(?TxManager $txm): array
    {
        if ($txm !== null) {
            return [true, $txm->getPdo()];
        }

        $ex = new DbException(null, "fail to get database connection");
        $pdo = PoolManager::getConnection('pdo');

        if (!($pdo instanceof PDO)) {
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

        if ($pdo === null) {
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

                $schema = collect($items)->map(function ($item) {
                    $fieldName = $item['Field'];
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

                    return compact(
                        'fieldName',
                        'fieldType',
                        'fieldSize',
                        'unsigned',
                        'nullable',
                        'defaultValue',
                        'autoIncrement',
                        'isPrimaryKey'
                    );
                })->toArray();
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

        $cacheFile = rtrim(self::$cacheDir, '/') . '/table_schemas.php';
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
        $logger = self::$logger;

        if (!($logger instanceof LoggerInterface) || !self::debugLogEnabled()) {
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
        $logger = self::$logger;

        if (!($logger instanceof LoggerInterface)) {
            return;
        }

        if ($msg instanceof Throwable) {
            $msg = ExceptionUtils::getStackTrace($msg);
        }

        $logger->error($msg);
    }
}
