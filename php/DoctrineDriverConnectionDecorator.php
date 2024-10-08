<?php
/**
 * Copyright (C) 2019 Gerrit Addiks.
 * This package (including this file) was released under the terms of the GPL-3.0.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/> or send me a mail so i can send you a copy.
 *
 * @license GPL-3.0
 * @author Gerrit Addiks <gerrit@addiks.de>
 */
namespace Addiks\DoctrineSqlAutoOptimizer;

use Addiks\StoredSQL\Exception\UnlexableSqlException;
use Addiks\StoredSQL\Exception\UnparsableSqlException;
use Addiks\StoredSQL\Schema\Schemas;
use Addiks\StoredSQL\Schema\SchemasClass;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\ParameterType;
use Monolog\Level;
use Monolog\Logger;
use Psr\SimpleCache\CacheInterface as PsrSimpleCache;
use Symfony\Contracts\Cache\CacheInterface as SymfonyCache;
use Throwable;
use Webmozart\Assert\InvalidArgumentException;

final class DoctrineDriverConnectionDecorator implements Connection
{
    public function __construct(
        private Connection $innerConnection,
        private SQLOptimizer $sqlOptimizer,
        private Logger $logger,
        private Schemas $schemas,
        private PsrSimpleCache|SymfonyCache|null $cache = null,
        private int|Level $errorDuringOptimizeLogLevel = Logger::NOTICE,
        private int|Level $queryOptimizedLogLevel = Logger::DEBUG
    ) {
    }

    public function prepare($sql)
    {
        /** @var string $optimizedSql */
        $optimizedSql = $this->optimizeSql($sql);

        try {
            return $this->innerConnection->prepare($optimizedSql);

        } catch (Throwable $exception) {
            $this->logger->notice(sprintf(
                'Optimized SQL query "%s" did result in exception "%s"! Trying again with original query "%s".',
                $optimizedSql,
                $exception->getMessage(),
                $sql
            ));

            return $this->innerConnection->prepare($sql);
        }
    }

    public function query()
    {
        /** @var array{0: string} $args */
        $args = func_get_args();

        /** @var array{0: string} $optimizedArgs */
        $optimizedArgs = $this->optimizeArgs($args);

        try {
            return call_user_func_array([$this->innerConnection, 'query'], $optimizedArgs);

        } catch (Throwable $exception) {
            $this->logger->notice(sprintf(
                'Optimized SQL query "%s" did result in exception "%s"! Trying again with original query "%s".',
                $optimizedArgs[0],
                $exception->getMessage(),
                $args[0]
            ));

            return call_user_func_array([$this->innerConnection, 'query'], $args);
        }
    }

    public function quote($value, $type = ParameterType::STRING)
    {
        return $this->innerConnection->quote($value, $type);
    }

    public function exec($sql): int
    {
        $optimizedSql = $this->optimizeSql($sql);

        try {
            return $this->innerConnection->exec($optimizedSql);

        } catch (Throwable $exception) {
            $this->logger->notice(sprintf(
                'Optimized SQL query "%s" did result in exception "%s"! Trying again with original query "%s".',
                $optimizedSql,
                $exception->getMessage(),
                $sql
            ));

            return $this->innerConnection->query($sql);
        }
    }

    public function lastInsertId($name = null)
    {
        return $this->innerConnection->lastInsertId($name);
    }

    public function beginTransaction()
    {
        return $this->innerConnection->beginTransaction();
    }

    public function commit()
    {
        return $this->innerConnection->commit();
    }

    public function rollBack()
    {
        return $this->innerConnection->rollBack();
    }

    public function errorCode()
    {
        return $this->innerConnection->errorCode();
    }

    public function errorInfo()
    {
        return $this->innerConnection->errorInfo();
    }

    /**
     * @param array{0: string} $args
     *
     * @return array{0: string}
     */
    private function optimizeArgs(array $args): array
    {
        /** @var string $sql */
        $sql = $args[0];

        /** @var mixed $optimizedSql */
        $optimizedSql = $this->optimizeSql($sql);

        /** @var array{0: string} $optimizedArgs */
        $optimizedArgs = $args;
        $optimizedArgs[0] = $optimizedSql;

        return $optimizedArgs;
    }

    private function optimizeSql(string $inputSql, bool $retryWithRefreshedCache = true): string
    {
        # Do not try to optimize anything other than SELECT's (for now at least).
        if (substr(trim($inputSql), 0, 6) !== 'SELECT') {
            return $inputSql;
        }

        if ($retryWithRefreshedCache && is_null($this->cache)) {
            $retryWithRefreshedCache = false;
        }

        /** @var string $outputSql */
        $outputSql = $inputSql;

        try {
            try {
                /** @var float $before */
                $before = microtime(true);

                /** @var string $outputSql */
                $outputSql = $this->sqlOptimizer->optimizeSql($inputSql, $this->schemas);

                /** @var float $after */
                $after = microtime(true);

                /** @var string $duration */
                $duration = number_format(($after - $before) * 1000, 2) . 'ms';

                if ($inputSql !== $outputSql) {
                    $this->logger->addRecord(
                        $this->queryOptimizedLogLevel,
                        sprintf('Optimized SQL "%s" to "%s", took %s.', $inputSql, $outputSql, $duration)
                    );
                }

            } catch (InvalidArgumentException $exception) {
                /** @var bool $isMissingTableException */
                $isMissingTableException = (1 === preg_match(
                    '/Table "[a-zA-Z0-9_-]+" not found in schema/is',
                    $exception->getMessage()
                ));

                if ($isMissingTableException && $retryWithRefreshedCache) {
                    $this->logger->addRecord(
                        Logger::DEBUG,
                        sprintf(
                            'Got a Table-is-Missing during SQL optimization, retrying with cleared cache (%s)',
                            $exception->getMessage()
                        )
                    );

                    SchemasClass::clearCache($pdo, $this->cache);
                    $this->schemas = SchemasClass::fromPDO($pdo, $this->cache);

                    $this->optimizeSql($inputSql, false);

                } else {
                    throw $exception;
                }
            }

        } catch (UnlexableSqlException | UnparsableSqlException $exception) {
            $this->logger->addRecord(
                $this->errorDuringOptimizeLogLevel,
                sprintf(
                    'Unable to optimize SQL query "%s" due to: %s',
                    $inputSql,
                    (string) $exception
                )
            );

        } catch (Throwable $exception) {
            $this->logger->addRecord(
                $this->errorDuringOptimizeLogLevel,
                sprintf(
                    'Unable to optimize SQL query "%s" due to: %s',
                    $inputSql,
                    (string) $exception
                )
            );
        }

        return $outputSql;
    }
}
