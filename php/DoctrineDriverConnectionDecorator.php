<?php
/**
 * Copyright (C) 2019 Gerrit Addiks.
 * This package (including this file) was released under the terms of the GPL-3.0.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/> or send me a mail so i can send you a copy.
 *
 * @license GPL-3.0
 *
 * @author Gerrit Addiks <gerrit@addiks.de>
 */

namespace Addiks\DoctrineSqlAutoOptimizer;

use Doctrine\DBAL\Driver\Connection;
use Monolog\Logger;
use Throwable;
use Addiks\StoredSQL\Schema\Schemas;

final class DoctrineDriverConnectionDecorator implements Connection
{
    public function __construct(
        private Connection $innerConnection,
        private SQLOptimizer $sqlOptimizer,
        private Logger $logger,
        private Schemas $schemas
    ) {
    }

    public function prepare(string $sql): Statement
    {
        return $this->innerConnection->prepare($sql);
    }

    public function query(string $sql): Result
    {
        $optimizedSql = $this->optimizeSql($sql);
        
        try {
            return $this->innerConnection->query($optimizedSql);

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

    public function quote($value, $type = ParameterType::STRING)
    {
        return $this->innerConnection->quote($value, $type);
    }

    public function exec(string $sql): int
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
    
    private function optimizeSql(string $inputSql): string
    {
        try {
            return $this->sqlOptimizer->optimizeSql($inputSql, $this->schemas);
            
        } catch (Throwable $exception) {
            $this->logger->notice(sprintf(
                'Unable to optimize SQL query "%s" due to: %s',
                $inputSql,
                (string) $exception
            ));
        }
    }
}
