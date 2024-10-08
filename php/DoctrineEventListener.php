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

use Addiks\StoredSQL\Schema\SchemasClass;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Event\ConnectionEventArgs;
use Monolog\Level;
use Monolog\Logger;
use PDO;
use Psr\SimpleCache\CacheInterface as PsrSimpleCache;
use ReflectionObject;
use ReflectionProperty;
use Symfony\Component\HttpKernel\CacheWarmer\CacheWarmerInterface;
use Symfony\Contracts\Cache\CacheInterface as SymfonyCache;
use Throwable;

final class DoctrineEventListener implements CacheWarmerInterface
{
    private SQLOptimizer $sqlOptimizer;

    private Logger $logger;

    private PsrSimpleCache|SymfonyCache|null $cache;

    public function __construct(
        Logger $logger,
        PsrSimpleCache|SymfonyCache|null $cache = null,
        ?SQLOptimizer $sqlOptimizer = null,
        private int|Level $errorDuringOptimizeLogLevel = Logger::NOTICE,
        private int|Level $queryOptimizedLogLevel = Logger::DEBUG,
        private int|Level $couldNotInitializeLogLevel = Logger::NOTICE,
    ) {
        $this->logger = $logger;
        $this->sqlOptimizer = $sqlOptimizer ?? (new DefaultSQLOptimizer($cache));
        $this->cache = $cache;

        if (isset($_SERVER['__ADDIKS_SQL_OPTIMIZER_DEBUG'])) {
            $this->errorDuringOptimizeLogLevel = Logger::WARNING;
            $this->queryOptimizedLogLevel = Logger::WARNING;
            $this->couldNotInitializeLogLevel = Logger::ERROR;
        }
    }

    public function postConnect(ConnectionEventArgs $event): void
    {
        try {
            /** @var Connection $connection */
            $connection = $event->getConnection();

            /** @var PDO|null $pdo */
            $pdo = $this->getPDOFromConnection($connection);

            /** @var SchemasClass|null $schemas */
            $schemas = $this->loadSchemasFromPDO($pdo);

            if (is_null($schemas)) {
                $this->logger->addRecord(
                    $this->couldNotInitializeLogLevel,
                    sprintf(
                        'The automatic SQL query optimization is only supported using PDO based connections, "%s" given.',
                        is_object($pdo) ? get_class($pdo) : gettype($pdo)
                    )
                );

                return;
            }

            $reflectionConnection = new ReflectionObject($connection);

            /** @var ReflectionProperty $reflectionDriverConnectionProperty */
            $reflectionDriverConnectionProperty = $reflectionConnection->getProperty('_conn');
            $reflectionDriverConnectionProperty->setAccessible(true);

            /** @var DriverConnection $oldConnection */
            $oldConnection = $reflectionDriverConnectionProperty->getValue($connection);

            /** @var DriverConnection $newConnection */
            $newConnection = new DoctrineDriverConnectionDecorator(
                $oldConnection,
                $this->sqlOptimizer,
                $this->logger,
                SchemasClass::fromPDO($pdo, $this->cache),
                $this->cache,
                $this->errorDuringOptimizeLogLevel,
                $this->queryOptimizedLogLevel
            );

            $reflectionDriverConnectionProperty->setValue($connection, $newConnection);

        } catch (Throwable $exception) {
            $this->logger->addRecord(
                $this->couldNotInitializeLogLevel,
                sprintf(
                    'Could not initialize automatic SQL query optimization: %s',
                    (string) $exception
                )
            );
        }
    }

    public function warmUp($cacheDir): array
    {
#        /** @var SchemasClass|null $schemas */
#        $schemas = $this->loadSchemasFromConnection($connection);
#
#        $this->sqlOptimizer->warmUpCacheFromSqlLog($schemas);
    }

    public function isOptional(): bool
    {
        return true;
    }

    private function getPDOFromConnection(Connection $connection): PDO|null
    {
        if (method_exists($connection, 'getNativeConnection')) {
            /** @var resource|object $pdo */
            $pdo = $connection->getNativeConnection();

        } else {
            /** @var DriverConnection $pdo */
            $pdo = $connection->getWrappedConnection();
        }

        if (!$pdo instanceof PDO) {
            return null;
        }

        return $pdo;
    }

    private function loadSchemasFromConnection(Connection $connection): SchemasClass|null
    {
        /** @var PDO|null $pdo */
        $pdo = $this->getPDOFromConnection($connection);

        return $this->loadSchemasFromPDO($pdo);
    }

    private function loadSchemasFromPDO(PDO|null $pdo): SchemasClass|null
    {
        if (is_null($pdo)) {
            return null;
        }

        return SchemasClass::fromPDO($pdo, $this->cache);
    }
}
