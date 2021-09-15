<?php

namespace mgboot\dal\redis;

use mgboot\common\Cast;

final class RedisConfig
{
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
    private $port = 6379;

    /**
     * @var string
     */
    private $password = '';

    /**
     * @var int
     */
    private $database = 0;

    /**
     * @var int
     */
    private $readTimeout = -1;

    /**
     * @var array|null
     */
    private $cliSettings = null;

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
        if (is_array($settings)) {
            if (is_string($settings['read-timeout']) && $settings['read-timeout'] !== '') {
                $settings['read-timeout'] = Cast::toDuration($settings['read-timeout']);
            }

            if (is_array($settings['cli-mode'])) {
                $settings['cliSettings'] = $settings['cli-mode'];
                unset($settings['cli-mode']);
            }
        }

        return new self($settings);
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

    /**
     * @return string
     */
    public function getPassword(): string
    {
        return $this->password;
    }

    /**
     * @return int
     */
    public function getDatabase(): int
    {
        return $this->database;
    }

    /**
     * @return int
     */
    public function getReadTimeout(): int
    {
        return $this->readTimeout;
    }

    /**
     * @return array|null
     */
    public function getCliSettings(): ?array
    {
        return $this->cliSettings;
    }
}
