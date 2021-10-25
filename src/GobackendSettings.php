<?php

namespace mgboot\dal;

use mgboot\common\swoole\Swoole;

final class GobackendSettings
{
    /**
     * @var array
     */
    private static $map1 = [];

    /**
     * @var bool
     */
    private $enabled;

    /**
     * @var string
     */
    private $host = '127.0.0.1';

    /**
     * @var int
     */
    private $port = -1;

    private function __construct(?array $settings = null)
    {
        if (!is_array($settings)) {
            $settings = [];
        }

        $enabled = false;

        foreach ($settings as $key => $value) {
            if (!is_string($key) || $key === '') {
                continue;
            }

            $pname = strtr($key, ['-' => ' ', '_' => ' ']);
            $pname = str_replace(' ', '', ucwords($pname));
            $pname = lcfirst($pname);

            if (!property_exists($this, $pname)) {
                continue;
            }

            $enabled = true;
            $this->$pname = $value;
        }

        $this->enabled = $enabled;
    }

    public static function create(?array $settings = null): self
    {
        return new self($settings);
    }

    public static function withSettings(GobackendSettings $settings, ?int $workerId = null): void
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "worker$workerId";
        } else {
            $key = 'noworker';
        }

        self::$map1[$key] = $settings;
    }

    public static function loadCurrent(?int $workerId = null): ?GobackendSettings
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "worker$workerId";
        } else {
            $key = 'noworker';
        }

        $settings = self::$map1[$key];
        return $settings instanceof GobackendSettings ? $settings : null;
    }

    /**
     * @return bool
     */
    public function isEnabled(): bool
    {
        return $this->enabled;
    }

    /**
     * @return string
     */
    public function getHost(): string
    {
        return $this->host;
    }

    /**
     * @return int
     */
    public function getPort(): int
    {
        return $this->port;
    }
}
