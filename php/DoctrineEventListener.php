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
use Monolog\Logger;
use PDO;
use Psr\SimpleCache\CacheInterface;
use ReflectionObject;
use ReflectionProperty;
use Throwable;

final class DoctrineEventListener
{
    private SQLOptimizer $sqlOptimizer;

    private Logger $logger;

    private ?CacheInterface $cache;

    public function __construct(
        Logger $logger,
        ?CacheInterface $cache = null,
        ?SQLOptimizer $sqlOptimizer = null
    ) {
        $this->logger = $logger;
        $this->sqlOptimizer = $sqlOptimizer ?? (new DefaultSQLOptimizer($cache));
        $this->cache = $cache;
    }

    public function postConnect(ConnectionEventArgs $event): void
    {
        try {
            /** @var Connection $connection */
            $connection = $event->getConnection();

            if (method_exists($connection, 'getNativeConnection')) {
                /** @var resource|object $pdo */
                $pdo = $connection->getNativeConnection();

            } else {
                /** @var DriverConnection $pdo */
                $pdo = $connection->getWrappedConnection();
            }

            if (!$pdo instanceof PDO) {
                $this->logger->notice(
                    'The automatic SQL query optimization is only supported using PDO based connections, "%s" given.',
                    is_object($pdo) ? get_class($pdo) : gettype($pdo)
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
                SchemasClass::fromPDO($pdo, $this->cache)
            );

            $reflectionDriverConnectionProperty->setValue($connection, $newConnection);

        } catch (Throwable $exception) {
            $this->logger->notice(sprintf(
                'Could not initialize automatic SQL query optimization: %s',
                (string) $exception
            ));
        }
    }
}
