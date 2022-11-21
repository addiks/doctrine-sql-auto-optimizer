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
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\ParameterType;
use Monolog\Logger;
use Throwable;

final class DoctrineDriverConnectionDecorator implements Connection
{
    public function __construct(
        private Connection $innerConnection,
        private SQLOptimizer $sqlOptimizer,
        private Logger $logger,
        private Schemas $schemas,
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

    private function optimizeSql(string $inputSql): string
    {
        # Do not try to optimize anything other than SELECT's (for now at least).
        if (substr(trim($inputSql), 0, 6) !== 'SELECT') {
            return $inputSql;
        }

        /** @var string $outputSql */
        $outputSql = $inputSql;

        try {
            /** @var string $outputSql */
            $outputSql = $this->sqlOptimizer->optimizeSql($inputSql, $this->schemas);

            if ($inputSql !== $outputSql) {
                $this->logger->addRecord(
                    $this->queryOptimizedLogLevel,
                    sprintf(
                        'Optimized SQL "%s" to "%s".',
                        $inputSql,
                        $outputSql
                    )
                );
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
