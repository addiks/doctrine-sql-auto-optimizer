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

use Addiks\DoctrineSqlAutoOptimizer\Mutators\RemovePointlessJoinsMutator;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstMutableNode;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstNode;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstRoot;
use Addiks\StoredSQL\Parsing\SqlParser;
use Addiks\StoredSQL\Parsing\SqlParserClass;
use Addiks\StoredSQL\Schema\Schemas;
use Closure;
use Psr\SimpleCache\CacheInterface;

/**
 * @psalm-import-type Mutator from SqlAstMutableNode
 * @psalm-import-type QueryOptimizedListener from SQLOptimizer
 *
 * @psalm-type MutatorWithSchemas = callable(SqlAstNode, int, SqlAstMutableNode, Schemas): void
 */
final class DefaultSQLOptimizer implements SQLOptimizer
{
    private SqlParser $sqlParser;

    /** @var array<MutatorWithSchemas> $mutators */
    private array $mutators;

    private ?CacheInterface $cache = null;

    /** @var list<QueryOptimizedListener> $listeners */
    private array $listeners = array();

    /** @param array<MutatorWithSchemas> $mutators */
    public function __construct(
        ?CacheInterface $cache = null,
        SqlParser $sqlParser = null,
        array $mutators = array(),
        bool $useDefaultMutators = true
    ) {
        if ($useDefaultMutators) {
            $mutators = array_merge($mutators, self::defaultMutators());
        }

        $this->sqlParser = $sqlParser ?? SqlParserClass::defaultParser();
        $this->mutators = $mutators;
        $this->cache = $cache;
    }

    /** @return array<MutatorWithSchemas> */
    public static function defaultMutators(): array
    {
        return [
            RemovePointlessJoinsMutator::create(),
        ];
    }

    public function optimizeSql(string $inputSql, Schemas $schemas): string
    {
        /** @var string $outputSql */
        $outputSql = '';

        /** @var string $cacheKey */
        $cacheKey = self::class . ':' . md5($inputSql);
        $cacheKey = preg_replace('/[\{\}\(\)\\\\\@\:]+/is', '_', $cacheKey);

        if (is_object($this->cache)) {
            $outputSql = $this->cache->get($cacheKey);

            if (!empty($outputSql)) {
                return $outputSql;
            }
        }

        /** @var SqlAstRoot $root */
        $root = $this->sqlParser->parseSql($inputSql);

        /**
         * @var array<Mutator> $mutators
         *
         * @param MutatorWithSchemas $mutator
         *
         * @return Mutator
         */
        $mutators = array_map(function (Closure $mutator) use ($schemas): Closure {
            return function (SqlAstNode $node, int $offset, SqlAstMutableNode $parent) use ($mutator, $schemas): void {
                $mutator($node, $offset, $parent, $schemas);
            };
        }, $this->mutators);

        /** @var string $beforeSql */
        $beforeSql = $root->toSql();

        $root->mutate($mutators);

        $outputSql = $root->toSql();

        if ($outputSql === $beforeSql) {
            $outputSql = $inputSql;
        }

        if (is_object($this->cache)) {
            $this->cache->set($cacheKey, $outputSql);
        }

        if ($inputSql !== $outputSql) {
            /** @var QueryOptimizedListener $listener */
            foreach ($this->listeners as $listener) {
                $listener($inputSql, $outputSql);
            }
        }

        return $outputSql;
    }

    /** @param QueryOptimizedListener $listener */
    public function addQueryOptimizedListener(callable $listener): void
    {
        $this->listeners[] = $listener;
    }
}
