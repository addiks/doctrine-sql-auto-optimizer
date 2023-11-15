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

use Addiks\DoctrineSqlAutoOptimizer\Mutators\RemovePointlessGroupByMutator;
use Addiks\DoctrineSqlAutoOptimizer\Mutators\RemovePointlessJoinsMutator;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstMutableNode;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstNode;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstRoot;
use Addiks\StoredSQL\Parsing\SqlParser;
use Addiks\StoredSQL\Parsing\SqlParserClass;
use Addiks\StoredSQL\Schema\Schemas;
use Closure;
use Psr\SimpleCache\CacheInterface;
use Throwable;
use Addiks\DoctrineSqlAutoOptimizer\Mutators\CountDistinctRemover;
use Addiks\DoctrineSqlAutoOptimizer\Mutators\SelectDistinctRemover;

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

    /** @var list<QueryOptimizedListener> $listeners */
    private array $listeners = array();

    /** @param array<MutatorWithSchemas> $mutators */
    public function __construct(
        private ?CacheInterface $cache = null,
        SqlParser $sqlParser = null,
        array $mutators = array(),
        bool $useDefaultMutators = true,
        public readonly string|null $optimizedSqlLogFilePath = null
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
            RemovePointlessGroupByMutator::create(),
            CountDistinctRemover::create(),
            SelectDistinctRemover::create(),
        ];
    }

    public function optimizeSql(string $inputSql, Schemas $schemas): string
    {
        /** @var string $outputSql */
        $outputSql = '';

        [$inputSql, $variables] = $this->normalizeSql($inputSql);

        /** @var string $cacheKey */
        $cacheKey = self::class . ':' . md5($inputSql);
        $cacheKey = preg_replace('/[\{\}\(\)\\\\\@\:]+/is', '_', $cacheKey);

        if (is_object($this->cache)) {
            $outputSql = $this->cache->get($cacheKey);
        }

        if (empty($outputSql) && !empty($inputSql)) {
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
                
                if (!empty($this->optimizedSqlLogFilePath)) {
                    file_put_contents(
                        $this->optimizedSqlLogFilePath,
                        base64_encode($inputSql) . "\n",
                        FILE_APPEND
                    );
                }
            }
        }

        $denormalizedOutputSql = $this->denormalizeSql($outputSql, $variables);

        if ($inputSql !== $outputSql) {
            /** @var QueryOptimizedListener $listener */
            foreach ($this->listeners as $listener) {
                $listener($inputSql, $denormalizedOutputSql);
            }
        }

        return $denormalizedOutputSql;
    }

    /** @param QueryOptimizedListener $listener */
    public function addQueryOptimizedListener(callable $listener): void
    {
        $this->listeners[] = $listener;
    }
    
    public function warmUpCacheFromSqlLog(Schemas $schemas): void
    {
        if (empty($this->optimizedSqlLogFilePath)) {
            return;
        }
        
        /** @var resource $read */
        $read = fopen($this->optimizedSqlLogFilePath, 'r');
        
        while ($b64 = fgets($read)) {
            try {
                /** @var string|false $sql */
                $sql = base64_decode($b64);
                
                if (is_string($sql)) {
                    $this->optimizeSql($sql, $schemas);
                }
                
            } catch (Throwable $exception) {
                continue;
            }
        }
        
        fclose($read);
    }

    /**
     * Normalize the input SQL by replacing all literals (which can vary from query to query) with fix variable-names
     * that do NOT vary from query to query. This way we do not pollute the cache with a gazillion nearly-equal queries
     * that just vary in what ID they query for.
     *
     * @return array{0: string, 1: array<string, string>}
     */
    private function normalizeSql(string $inputSql): array
    {
        if (substr_count($inputSql, "'") % 2 === 1 || substr_count($inputSql, '"') % 2 === 1) {
            # An uneven number of quotes indicates that a string contains an escaped quote,
            # which this simple mechanism cannot (yet) deal with.
            return $inputSql;
        }

        /** @var array<string, string> $variables */
        $variables = array();

        /** @var int $counter */
        $counter = 0;

        /** @var string|null $outputSql */
        $outputSql = preg_replace_callback(
            '/(\"[^\"]*\")|(\'[^\']*\')|(?<![a-zA-Z0-9_])([0-9]+(\.[0-9]+)?)/is',
            function (array $matches) use (&$variables, &$counter): string {
                /** @var string $sql */
                $sql = $matches[0];

                /** @var string $key */
                $key = ':LITERAL_' . str_pad((string) $counter, 6, '0', STR_PAD_LEFT);
                $counter++;

                $variables[$key] = $sql;

                return $key;
            },
            $inputSql
        );

        if (is_null($outputSql)) {
            $outputSql = $inputSql;
            $variables = array();
        }

        return [$outputSql, $variables];
    }

    /** @param array<string, string> $variables */
    private function denormalizeSql(string $inputSql, array $variables): string
    {
        if (empty($variables)) {
            return $inputSql;
        }

        /** @var string|null $outputSql */
        $outputSql = preg_replace_callback(
            '/\:LITERAL_[0-9]{6}/is',
            function (array $matches) use ($variables): string {
                return $variables[$matches[0]] ?? $matches[0];
            },
            $inputSql
        );

        return $outputSql ?? $inputSql;
    }
}
