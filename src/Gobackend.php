<?php

namespace mgboot\dal;

use mgboot\common\Cast;
use mgboot\common\swoole\Swoole;

final class Gobackend
{
    private function __construct()
    {
    }

    /**
     * @param string $msg
     * @param int|null $readBufferSize
     * @param int|string|null $timeout
     * @return string
     */
    public static function send(string $msg, ?int $readBufferSize = null, $timeout = null): string
    {
        $settings = GobackendSettings::loadCurrent();

        if (!($settings instanceof GobackendSettings) ||
            !$settings->isEnabled() ||
            $settings->getHost() === '' ||
            $settings->getPort() < 1) {
            return '';
        }

        if (!is_int($readBufferSize)) {
            $readBufferSize = 64 * 1024;
        }

        if (is_string($timeout) && $timeout !== '') {
            $timeout = Cast::toDuration($timeout);
        }

        if (!is_int($timeout) || $timeout < 1) {
            $timeout = 2;
        }

        if (Swoole::inCoroutineMode(true)) {
            return self::sendAsync([$settings->getHost(), $settings->getPort(), $msg, $readBufferSize, $timeout]);
        }

        $fp = fsockopen($settings->getHost(), $settings->getPort());

        if (!is_resource($fp)) {
            return '';
        }

        fwrite($fp, $msg);
        stream_set_timeout($fp, $timeout);
        $result = '';

        while (!feof($fp)) {
            $buf = fread($fp, $readBufferSize);
            $info = stream_get_meta_data($fp);

            if ($info['timed_out']) {
                return '';
            }

            if (!is_string($buf)) {
                continue;
            }

            $result .= $buf;
        }

        return $result;
    }

    private static function sendAsync(array $payloads): string
    {
        list($host, $port, $msg, $readBufferSize, $timeout) = $payloads;
        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        $socket = new \Swoole\Coroutine\Socket(AF_INET, SOCK_STREAM, 0);

        if ($socket->connect($host, $port, 0.5) !== true) {
            return '';
        }

        $n1 = $socket->sendAll($msg);

        if (!is_int($n1) || $n1 < strlen($msg)) {
            $socket->close();
            return '';
        }

        $result = $socket->recvAll($readBufferSize, floatval($timeout));
        $socket->close();
        return is_string($result) ? $result : '';
    }
}
