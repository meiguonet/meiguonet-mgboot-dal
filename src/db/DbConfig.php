<?php

namespace mgboot\dal\db;

final class DbConfig
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
    private $port = 3306;

    /**
     * @var string
     */
    private $username = 'root';

    /**
     * @var string
     */
    private $password = '';

    /**
     * @var string
     */
    private $dbname = '';

    /**
     * @var string
     */
    private $charset = 'utf8mb4';

    /**
     * @var string
     */
    private $collation = 'utf8mb4_general_ci';

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
            if (is_string($settings['database'])) {
                $settings['dbname'] = $settings['database'];
                unset($settings['database']);
            }

            if (is_array($settings['cliMode'])) {
                $settings['cliSettings'] = $settings['cliMode'];
                unset($settings['cli-mode']);
            } else if (is_array($settings['cli-mode'])) {
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
    public function getUsername(): string
    {
        return $this->username;
    }

    /**
     * @return string
     */
    public function getPassword(): string
    {
        return $this->password;
    }

    /**
     * @return string
     */
    public function getDbname(): string
    {
        return $this->dbname;
    }

    /**
     * @return string
     */
    public function getCharset(): string
    {
        return $this->charset;
    }

    /**
     * @return string
     */
    public function getCollation(): string
    {
        return $this->collation;
    }

    /**
     * @return array|null
     */
    public function getCliSettings(): ?array
    {
        return $this->cliSettings;
    }
}
