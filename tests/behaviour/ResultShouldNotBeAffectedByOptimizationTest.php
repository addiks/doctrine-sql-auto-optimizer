<?php
/**
 * Copyright (C) 2019  Gerrit Addiks.
 * This package (including this file) was released under the terms of the GPL-3.0.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/> or send me a mail so i can send you a copy.
 * @license GPL-3.0
 * @author Gerrit Addiks <gerrit@addiks.de>
 */

namespace Addiks\DoctrineSqlAutoOptimizer\Tests\Behaviour;

use PHPUnit\Framework\TestCase;
use Addiks\DoctrineSqlAutoOptimizer\DefaultSQLOptimizer;
use Addiks\StoredSQL\Schema\Schemas;
use Addiks\StoredSQL\Schema\SchemasClass;
use Psr\SimpleCache\CacheInterface;
use PDO;
use PDOStatement;
use Closure;
use DateTime;
use Throwable;

final class ResultShouldNotBeAffectedByOptimizationTest extends TestCase
{

    private DefaultSQLOptimizer $optimizer;

    private CacheInterface $cache;

    private PDO|null $pdo = null;

    /*** @var array<string, array<int, string>> */
    private array $ids = array();

    private Schemas $schemas;

    public function __construct(?string $name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);

        $this->cache = new class implements CacheInterface {
            /** @var array<string, string> */
            private array $cachedItems = array();

            public function get(string $key, mixed $default = null): mixed
            {
                return $this->cachedItems[$key] ?? $default;
            }

            public function set(string $key, mixed $value, null|int|\DateInterval $ttl = null): bool
            {
                $this->cachedItems[$key] = $value;

                return true;
            }

            public function delete(string $key): bool
            {
                unset($this->cachedItems[$key]);
            }

            public function clear(): bool
            {
                $this->cachedItems = array();
            }

            public function getMultiple(iterable $keys, mixed $default = null): iterable
            {
                return array_combine($keys, array_map(function (string $key) use ($default): mixed {
                    return $this->cachedItems[$key] ?? $default;
                }, $keys));
            }

            public function setMultiple(iterable $values, null|int|\DateInterval $ttl = null): bool
            {
                foreach ($values as $key => $value) {
                    $this->set($key, $value, $ttl);
                }
            }

            public function deleteMultiple(iterable $keys): bool
            {
                foreach ($keys as $key) {
                    $this->delete($key);
                }
            }

            public function has(string $key): bool
            {
                return array_key_exists($this->cachedItems, $key);
            }

        };

        $this->optimizer = new DefaultSQLOptimizer($this->cache);
    }

    public function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');

        $this->createSchema();

        $this->schemas = SchemasClass::fromPDO($this->pdo);

        $this->insertTestFixtures();
    }

    public function tearDown(): void
    {
        $this->pdo = null;
        gc_collect_cycles();
    }

    /**
     * @test
     * @dataProvider generateTestData
     */
    public function resultShouldNotBeAffectedByOptimization(string $originalSql, bool $expectSqlChange = null): void
    {
        if ($this->pdo->getAttribute(PDO::ATTR_DRIVER_NAME) === 'sqlite') {
            if (is_int(strpos($originalSql, 'RIGHT JOIN'))) {
                $this->markTestSkipped('Sqlite apparently does not support RIGHT JOIN?!');
            }
            if (is_int(strpos($originalSql, 'OUTER JOIN'))) {
                $this->markTestSkipped('Sqlite also does not support OUTER join .. :-/');
            }
        }
        
        /** @var string $optimizedSql */
        $optimizedSql = $this->optimizer->optimizeSql($originalSql, $this->schemas);
        
        if ($expectSqlChange) {
            $this->assertNotEquals($originalSql, $optimizedSql);
            
        } elseif ($expectSqlChange === false) {
            $this->assertEquals($originalSql, $optimizedSql);
        }

        /** @var PDOStatement|false $originalStatement */
        $originalStatement = $this->pdo->prepare($originalSql);

        /** @var PDOStatement|false $optimizedStatement */
        $optimizedStatement = $this->pdo->prepare($optimizedSql);

        $this->assertTrue(is_object($originalStatement));
        $this->assertTrue(is_object($optimizedStatement));

        $originalStatement->execute();
        $optimizedStatement->execute();

        /** @var array<array<string, string>> $expectedResult */
        $expectedResult = $originalStatement->fetchAll(PDO::FETCH_ASSOC);

        /** @var array<array<string, string>> $actualResult */
        $actualResult = $optimizedStatement->fetchAll(PDO::FETCH_ASSOC);

        try {
            $this->assertEquals($expectedResult, $actualResult);
            
        } catch (Throwable $exception) {
            echo sprintf(
                "\nOriginal (expected) SQL: <%s>, Optimized (actual) SQL: <%s>",
                $originalSql,
                $optimizedSql
            );
            
            throw $exception;
        }
    }

    public function generateTestData(): array
    {
        /** @var array<array{0: string}> $tests */
        $tests = array();

        ### Statements that should not be changed during optimization:

        # Non-Nullable ONE-to-ONE
        $tests[] = [<<<SQL
            SELECT *
            FROM ratings r
            LEFT JOIN sales s ON(s.id = r.sale_id)
            SQL, false];

        # Nullable ONE-to-ONE
        $tests[] = [<<<SQL
            SELECT *
            FROM articles a
            LEFT JOIN articles b ON(a.id = b.successed_by)
            SQL, false];

        # Non-Nullable ONE-to-MANY
        $tests[] = [<<<SQL
            SELECT *
            FROM sales s
            LEFT JOIN sale_items i ON(s.id = i.sale_id)
            SQL, false];

        # Nullable ONE-to-MANY
        $tests[] = [<<<SQL
            SELECT *
            FROM sales s
            LEFT JOIN payments p ON(s.id = p.sale_id)
            SQL, false];

        # Many-To-Many
        $tests[] = [<<<SQL
            SELECT *
            FROM sales s
            LEFT JOIN sale_items i ON(s.id = i.sale_id)
            LEFT JOIN articles a ON(a.id = i.article_id)
            SQL, false];

        ### Statements that would be optimized, if it were not for a change in result-set:

        # ...


        ### Statements that will be optimized without a change to result-set:

        # Non-Nullable ONE-to-ONE
        $tests[] = [<<<SQL
            SELECT r.*
            FROM ratings r
            LEFT JOIN sales s ON(s.id = r.sale_id)
            SQL, true];
            
            
        ### Different JOIN-Types

        $tables = [
            'customers' => [
                'sales' => 'customer_id'
            ],
            'articles' => [
                'sale_items' => 'article_id',
                'articles' => 'successed_by',
            ],
            'sales' => [
                'sale_items' => 'sale_id',
                'ratings' => 'sale_id',
                'payments' => 'sale_id',
            ],
            'sale_items' => [],
            'payments' => [],
            'ratings' => [],
        ];

        foreach([
            'INNER JOIN', 
            'LEFT JOIN', 
            'RIGHT JOIN',
            'OUTER JOIN',
        ] as $joinType) {
            foreach ($tables as $leftTable => $relations) {
                foreach ($relations as $rightTable => $aliasOfLeftTable) {

                    foreach ([
                        [$leftTable, 'id', $rightTable, $aliasOfLeftTable],
                        [$rightTable, $aliasOfLeftTable, $leftTable, 'id'],
                    ] as [$aTable, $aRefColumn, $bTable, $bRefColumn]) {
                        
                        foreach ([
                            'a.*' => true, 
                            '*' => false, 
                            'b.*' => true,
                        ] as $columns => $expectSqlChange) {
                            
                            $sql = sprintf(
                                <<<SQL
                                SELECT %s
                                FROM %s a
                                %s %s b ON(a.%s = b.%s)
                                SQL, 
                                $columns,
                                $aTable,
                                $joinType,
                                $bTable,
                                $aRefColumn,
                                $bRefColumn
                            );
                            
                            $tests[$sql] = [$sql];
                        }
                    }
                }
            }
        }

        return $tests;
    }

    private function createSchema(): void
    {
        # A small sample DB with all relation-type constallations present:
        #
        #   TYPE          | NULLABLE | NOT NULLABLE
        #  ---------------+----------+-----------------------------
        #   ONE-to-ONE:   |       NO | ratings-to-sales (sale_id)
        #   ONE-to-ONE:   |      YES | articles-to-articles (successed_by)
        #   ONE-to-MANY:  |       NO | customers-to-sales (customer_id)
        #   ONE-to-MANY:  |      YES | sales-to-payments (sale_id)
        #   MANY-to-MANY: |      N/A | sales-to-articles (sale_items)

        $this->pdo->query('
            CREATE TABLE `customers` (
                `id` VARCHAR(32) NOT NULL PRIMARY KEY,
                `name` VARCHAR(64) NOT NULL,
                `address` SMALLTEXT
            );
        ');

        $this->pdo->query('
            CREATE TABLE `sales` (
                `id` VARCHAR(32) NOT NULL PRIMARY KEY,
                `invoice_number` INT NOT NULL,
                `customer_id` VARCHAR(32) NOT NULL,
                `purchase_date` DATETIME NOT NULL
            );
        ');

        $this->pdo->query('
            CREATE TABLE `sale_items` (
                `id` VARCHAR(32) NOT NULL PRIMARY KEY,
                `sale_id` VARCHAR(32) NOT NULL,
                `article_id` VARCHAR(32) NOT NULL,
                `quantity` INT DEFAULT 1,
                `price` INT DEFAULT 0
            );
        ');

        $this->pdo->query('
            CREATE TABLE `ratings` (
                `id` VARCHAR(32) NOT NULL PRIMARY KEY,
                `sale_id` VARCHAR(32) NOT NULL UNIQUE,
                `rating` INT NOT NULL
            );
        ');

        $this->pdo->query('
            CREATE TABLE `articles` (
                `id` VARCHAR(32) NOT NULL PRIMARY KEY,
                `name` VARCHAR(64) NOT NULL,
                `purchase_price` INT NOT NULL,
                `selling_price` INT NOT NULL,
                `successed_by` VARCHAR(32) UNIQUE
            );
        ');

        $this->pdo->query('
            CREATE TABLE `payments` (
                `id` VARCHAR(32) NOT NULL PRIMARY KEY,
                `sale_id` VARCHAR(32),
                `paid_amount` INT NOT NULL,
                `reference` VARCHAR(64) NOT NULL
            );
        ');
    }

    private function insertTestFixtures(): void
    {
        self::label(true);

        /*** @var array<string, array<int, string>> */
        $ids = $this->generateRowIds([
            'articles' => 5,
            'customers' => 2,
            'sales' => 4,
            'sale_items' => 8,
            'payments' => 4,
            'ratings' => 3,
        ]);

        $this->ids = $ids;

        $this->pdo->query('
            INSERT INTO `articles`
            (`id`, `name`, `purchase_price`, `selling_price`, `successed_by`)
            VALUES
            ("' . $ids['articles'][0] . '", "' . self::label() . '", 12300,  9900, NULL),
            ("' . $ids['articles'][1] . '", "' . self::label() . '", 12300, 12900, "' . $ids['articles'][0] . '"),
            ("' . $ids['articles'][2] . '", "' . self::label() . '", 12300,  4990, NULL),
            ("' . $ids['articles'][3] . '", "' . self::label() . '", 12300,  5500, "' . $ids['articles'][2] . '"),
            ("' . $ids['articles'][4] . '", "' . self::label() . '", 12300, 28850, NULL);
        ');

        $this->pdo->query('
            INSERT INTO `customers`
            (`id`, `name`, `address`)
            VALUES
            ("' . $ids['customers'][0] . '", "' . self::label() . '", "' . self::label() . '"),
            ("' . $ids['customers'][1] . '", "' . self::label() . '", NULL);
        ');

        $this->pdo->query('
            INSERT INTO `sales`
            (`id`, `invoice_number`, `customer_id`, `purchase_date`)
            VALUES
            ("' . $ids['sales'][0] . '", "' . self::label() . '", "' . $ids['customers'][0] . '", "' . self::date() . '"),
            ("' . $ids['sales'][1] . '", "' . self::label() . '", "' . $ids['customers'][0] . '", "' . self::date() . '"),
            ("' . $ids['sales'][2] . '", "' . self::label() . '", "' . $ids['customers'][1] . '", "' . self::date() . '"),
            ("' . $ids['sales'][3] . '", "' . self::label() . '", "' . $ids['customers'][1] . '", "' . self::date() . '");
        ');

        $this->pdo->query('
            INSERT INTO `sale_items`
            (`id`, `sale_id`, `article_id`, `quantity`, `price`)
            VALUES
            ("' . $ids['sale_items'][0] . '", "' . $ids['sales'][0] . '", "' . $ids['articles'][0] . '", 1,  3300),
            ("' . $ids['sale_items'][1] . '", "' . $ids['sales'][1] . '", "' . $ids['articles'][1] . '", 1, 10900),
            ("' . $ids['sale_items'][2] . '", "' . $ids['sales'][1] . '", "' . $ids['articles'][0] . '", 4, 21200),
            ("' . $ids['sale_items'][3] . '", "' . $ids['sales'][1] . '", "' . $ids['articles'][2] . '", 1, 13500),
            ("' . $ids['sale_items'][4] . '", "' . $ids['sales'][2] . '", "' . $ids['articles'][3] . '", 3,  1340),
            ("' . $ids['sale_items'][5] . '", "' . $ids['sales'][2] . '", "' . $ids['articles'][2] . '", 2,  6240),
            ("' . $ids['sale_items'][6] . '", "' . $ids['sales'][3] . '", "' . $ids['articles'][3] . '", 1, 26400),
            ("' . $ids['sale_items'][7] . '", "' . $ids['sales'][3] . '", "' . $ids['articles'][4] . '", 1,  2134);
        ');

        $this->pdo->query('
            INSERT INTO `payments`
            (`id`, `sale_id`, `paid_amount`, `reference`)
            VALUES
            ("' . $ids['payments'][0] . '", "' . $ids['sales'][0] . '", 28373, "' . self::label() . '"),
            ("' . $ids['payments'][1] . '", "' . $ids['sales'][1] . '", 24632, "' . self::label() . '"),
            ("' . $ids['payments'][2] . '", "' . $ids['sales'][2] . '",  9743, "' . self::label() . '"),
            ("' . $ids['payments'][3] . '", "' . $ids['sales'][2] . '", 13879, "' . self::label() . '");
        ');

        $this->pdo->query('
            INSERT INTO `ratings`
            (`id`, `sale_id`, `rating`)
            VALUES
            ("' . $ids['ratings'][0] . '", "' . $ids['sales'][0] . '", 3),
            ("' . $ids['ratings'][1] . '", "' . $ids['sales'][1] . '", 1),
            ("' . $ids['ratings'][2] . '", "' . $ids['sales'][3] . '", 5);
        ');
    }

    /*** @return array<string, array<int, string>> */
    private static function generateRowIds(array $tableDef): array
    {
        return array_combine(
            array_keys($tableDef),
            array_map(function (int $idCount): array {
                return array_map(function (): string {
                    return self::label(32);
                }, range(1, $idCount));
            }, $tableDef)
        );
    }

    private static function label(int $length = 8, bool $reset = false): string
    {
        static $currentLabel = '';

        if ($reset) {
            $currentLabel = '';
        }

        $currentLabel = md5($currentLabel);

        /** @var string $newLabel */
        $newLabel = substr($currentLabel, 0, $length);

        return $newLabel;
    }

    private static function date(): string
    {
        /** @var int $timestamp */
        $timestamp = hexdec(substr(self::label(), 0, 8));

        $date = new DateTime();
        $date->setTimestamp($timestamp);

        return $date->format('Y-m-d H:i:s');
    }
}
