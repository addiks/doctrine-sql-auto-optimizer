<?php
/**
 * Copyright (C) 2019  Gerrit Addiks.
 * This package (including this file) was released under the terms of the GPL-3.0.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/> or send me a mail so i can send you a copy.
 *
 * @license GPL-3.0
 * @author Gerrit Addiks <gerrit@addiks.de>
 */

namespace Addiks\DoctrineSqlAutoOptimizer\Tests\Behaviour;

use Addiks\DoctrineSqlAutoOptimizer\DefaultSQLOptimizer;
use Addiks\StoredSQL\Schema\Schemas;
use Addiks\StoredSQL\Schema\SchemasClass;
use DateTime;
use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Psr\SimpleCache\CacheInterface;
use Throwable;
use Addiks\StoredSQL\Schema\SchemaClass;
use Addiks\StoredSQL\Schema\TableClass;
use Addiks\StoredSQL\Schema\ColumnClass;
use Addiks\StoredSQL\Types\SqlTypeClass;

final class BenchmarkingTest extends TestCase
{
    private static DefaultSQLOptimizer $optimizer;

    /** @var array<string, array<int, string>> */
    private static array $ids = array();

    private static array $statistics;

    private static Schemas $schemas;
    
    private const COLUMN_COUNT = 1024;

    public static function setUpBeforeClass(): void
    {
        self::$optimizer = new DefaultSQLOptimizer();

        self::$schemas = new SchemasClass();
        
        $schema = new SchemaClass(self::$schemas, 'sample_schema');
        
        $table = new TableClass($schema, 'foo');
        
        /** @var SqlTypeClass $int */
        $int = SqlTypeClass::fromName('INT');
        
        for ($i = 0; $i < self::COLUMN_COUNT; $i++) {
            new ColumnClass($table, 'c' . $i, $int, false, false);
        }

        self::$statistics = [
            'all' => 0,
            'changed' => 0,
            'unchanged' => 0,
        ];
    }

    public static function tearDownAfterClass(): void
    {
        echo sprintf(
            "\n\nAll optimized queries:                 %s",
            str_pad(self::$statistics['all'], 3, ' ', STR_PAD_LEFT)
        );

        echo sprintf(
            "\nChanged queries:                       %s (%d%%)",
            str_pad(self::$statistics['changed'], 3, ' ', STR_PAD_LEFT),
            (self::$statistics['changed'] / self::$statistics['all'] * 100)
        );

        echo sprintf(
            "\nUnchanged queries:                     %s (%d%%)",
            str_pad(self::$statistics['unchanged'], 3, ' ', STR_PAD_LEFT),
            (self::$statistics['unchanged'] / self::$statistics['all'] * 100)
        );
    }

    /**
     * @test
     *
     * @dataProvider generateTestData
     */
    public function benchmark(string $originalSql): void
    {
        self::$statistics['all']++;

        /** @var string $optimizedSql */
        $optimizedSql = self::$optimizer->optimizeSql($originalSql, self::$schemas);

        if ($originalSql !== $optimizedSql) {
            self::$statistics['changed']++;

        } else {
            self::$statistics['unchanged']++;
        }
    }

    public function generateTestData(): array
    {
        $sql = sprintf(
            'SELECT %s FROM foo', 
            implode(', ', array_map(
                fn ($i) => 'c' . $i, 
                range(0, self::COLUMN_COUNT - 1)
            ))
        );
        
        return [
            '0' => [$sql],
        ];
        
        
        /** @var array<array{0: string}> $tests */
        $tests = array();

        $counter = 0;

        foreach ([
            'JOIN',
            'INNER JOIN',
            'CROSS JOIN',
            'LEFT JOIN',
            'RIGHT JOIN',
            # 'FULL OUTER JOIN' # This is only relevant on MS-SQL databases. (I dont have one to test on.)
        ] as $joinType) {
            foreach (self::RELATIONSHIPS as $leftTable => $relations) {
                foreach ($relations as [$rightTable, $aliasOfLeftTable]) {
                    foreach ([
                        [$leftTable, 'id', $rightTable, $aliasOfLeftTable],
                        [$rightTable, $aliasOfLeftTable, $leftTable, 'id'],
                    ] as [$aTable, $aRefColumn, $bTable, $bRefColumn]) {
                        foreach ([
                            'a.*',
                            '*',
                            'b.*',
                        ] as $columns) {
                            $sql = sprintf(
                                'SELECT %s FROM %s a %s %s b ON(a.%s = b.%s)',
                                $columns,
                                $aTable,
                                $joinType,
                                $bTable,
                                $aRefColumn,
                                $bRefColumn
                            );

                            $tests[str_pad($counter, 3, '0', STR_PAD_LEFT) . ':' . $sql] = [$sql];
                            $counter++;
                        }
                    }
                }
            }
        }

        return $tests;
    }


}
