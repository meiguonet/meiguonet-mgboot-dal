<?php

namespace mgboot\dal\db;

use mgboot\dal\Connection;
use PDO;

final class TxManager
{
    /**
     * @var PDO
     */
    private $pdo;

    private function __construct($conn)
    {
        $msg = sprintf(
            'TxManager constructor arg0 require a PDO object or a %s object with a PDO object as real connection',
            Connection::class
        );

        if (!is_object($conn)) {
            throw new DbException(null, $msg);
        }

        if ($conn instanceof Connection) {
            $pdo = $conn->getPdo();

            if (!is_object($pdo)) {
                throw new DbException(null, $msg);
            }

            $this->pdo = $pdo;
            return;
        }

        if (!($conn instanceof PDO)) {
            throw new DbException(null, $msg);
        }

        $this->pdo = $conn;
    }

    public static function create($conn): self
    {
        return new self($conn);
    }

    public function getPdo(): PDO
    {
        return $this->pdo;
    }
}
