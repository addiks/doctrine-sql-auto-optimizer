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

    private static DefaultSQLOptimizer $optimizer;

    private static CacheInterface $cache;

    private static PDO|null $pdo = null;

    /*** @var array<string, array<int, string>> */
    private static array $ids = array();

    private static Schemas $schemas;

    public function __construct(?string $name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);
        
    }
    
    public static function setUpBeforeClass(): void
    {
        self::$cache = new class implements CacheInterface {
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
                
                return true;
            }

            public function clear(): bool
            {
                $this->cachedItems = array();
                
                return true;
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
                
                return true;
            }

            public function deleteMultiple(iterable $keys): bool
            {
                foreach ($keys as $key) {
                    $this->delete($key);
                }
                
                return true;
            }

            public function has(string $key): bool
            {
                return array_key_exists($this->cachedItems, $key);
            }

        };

        self::$optimizer = new DefaultSQLOptimizer(self::$cache);
        
        self::$pdo = new PDO(
            $_SERVER['PDO_DSN'] ?? 'sqlite::memory:', 
            $_SERVER['PDO_USER'] ?? null, 
            $_SERVER['PDO_PASSWORD'] ?? null
        );

        self::createSchema();

        self::$schemas = SchemasClass::fromPDO(self::$pdo);

        self::insertTestFixtures();
    }

    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
        #self::$pdo = null;
       # gc_collect_cycles();
    }

    /**
     * @test
     * @dataProvider generateTestData
     */
    public function resultShouldNotBeAffectedByOptimization(string $originalSql, bool $expectSqlChange = null): void
    {
        if (self::$pdo->getAttribute(PDO::ATTR_DRIVER_NAME) === 'sqlite') {
            if (is_int(strpos($originalSql, 'RIGHT JOIN'))) {
                $this->markTestSkipped('Sqlite does not support RIGHT JOIN.');
            }
        }
            
        /** @var array<array<string, string>> $expectedResult */
        $expectedResult = $this->query($originalSql);

        /** @var string $optimizedSql */
        $optimizedSql = self::$optimizer->optimizeSql($originalSql, self::$schemas);

        if ($originalSql !== $optimizedSql) {
            # If the query was changed, make sure it did not affect the result-set
            
            /** @var array<array<string, string>> $actualResult */
            $actualResult = $this->query($optimizedSql);

            try {
                $this->assertResultsSetsAreEqual($expectedResult, $actualResult);
                
            } catch (Throwable $exception) {
                echo sprintf(
                    "\nOriginal  (expected) SQL: <%s>\nOptimized (actual)   SQL: <%s>",
                    $originalSql,
                    $optimizedSql
                );
                
                throw $exception;
            }
            
        } elseif ($expectSqlChange === null) {
            # If the query was NOT changed, make sure that a change would have affected the result-set
            
            /** @var string $cacheKey */
            $cacheKey = DefaultSQLOptimizer::class . ':' . md5($originalSql);
            $cacheKey = preg_replace('/[\{\}\(\)\\\\\@\:]+/is', '_', $cacheKey);

            self::$cache->delete($cacheKey);
            
            $GLOBALS['__ADDIKS_DEBUG_IGNORE_JOIN_REMOVAL_CHECK'] = true;
            $optimizedSql = self::$optimizer->optimizeSql($originalSql, self::$schemas);
            unset($GLOBALS['__ADDIKS_DEBUG_IGNORE_JOIN_REMOVAL_CHECK']);

            /** @var array<array<string, string>> $actualResult */
            $actualResult = $this->query($optimizedSql);

            try {
                $this->assertResultsSetsAreNotEqual($expectedResult, $actualResult);
                
            } catch (Throwable $exception) {
                echo sprintf(
                    "\nOriginal  (expected) SQL: <%s>\nOptimized (actual)   SQL: <%s>",
                    $originalSql,
                    $optimizedSql
                );
                
                throw $exception;
            }
        }
    }
    
    private function assertResultsSetsAreEqual(array $expectedResult, array $actualResult): void
    {
        $this->assertTrue(
            $this->resultSetContainsResultSet($expectedResult, $actualResult) && 
            $this->resultSetContainsResultSet($actualResult, $expectedResult),
            'Result-Sets are NOT equal! They were expected to be equal!'
        );
    }
    
    private function assertResultsSetsAreNotEqual(array $expectedResult, array $actualResult): void
    {
        $this->assertFalse(
            $this->resultSetContainsResultSet($expectedResult, $actualResult) &&
            $this->resultSetContainsResultSet($actualResult, $expectedResult),
            'Result-Sets are equal! They were expected to NOT be equal!'
        );
    }
    
    private function resultSetContainsResultSet(array $haystack, array $needle): bool
    {
        if (count($needle) > count($haystack)) {
            return false;
        }
        
        foreach ($needle as $needleRow) {
            foreach ($haystack as $haystackRow) {
                if (serialize($needleRow) === serialize($haystackRow)) {
                    continue 2;
                }
            }

            return false;
        }

        return true;
    }
    
    private function query(string $sql): array
    {
        /** @var PDOStatement|false $statement */
        $statement = self::$pdo->prepare($sql);
        
        $this->assertTrue(is_object($statement));

        $statement->execute();

        return $statement->fetchAll(PDO::FETCH_ASSOC);
    }

    public function generateTestData(): array
    {
        /** @var array<array{0: string}> $tests */
        $tests = array();

        $tables = [
            #'customers' => [
            #    ['sales', 'customer_id']
            #],
            #'articles' => [
            #    ['sale_items', 'article_id'],
            #    ['articles', 'successed_by'],
            #],
            #'sales' => [
            #    ['sale_items', 'sale_id'],
            #    ['ratings', 'sale_id'],
            #    ['payments', 'sale_id'],
            #],
            #'sale_items' => [],
            #'payments' => [],
            #'ratings' => [],
            'test2' => [
                ['test1', 'null_unique'],
                ['test1', 'null_notunique'],
                ['test1', 'notnull_notunique'],
            ],
            'test1' => [
                ['test2', 'null_unique'],
                ['test2', 'null_notunique'],
                ['test2', 'notnull_unique'],
                ['test2', 'notnull_notunique'],
            ]
        ];
        
        $counter = 0;

        foreach([
            'JOIN',
            'INNER JOIN',
            'CROSS JOIN',
            'LEFT JOIN',
            'RIGHT JOIN',
            # 'FULL OUTER JOIN' # This is only relevant on MS-SQL databases. (I dont have one to test on.)
        ] as $joinType) {
            foreach ($tables as $leftTable => $relations) {
                foreach ($relations as [$rightTable, $aliasOfLeftTable]) {

                    foreach ([
                        [$leftTable, 'id', $rightTable, $aliasOfLeftTable],
                        [$rightTable, $aliasOfLeftTable, $leftTable, 'id'],
                    ] as [$aTable, $aRefColumn, $bTable, $bRefColumn]) {
                        
                        foreach ([
                            'a.*' => null, 
                            '*' => false, 
                            'b.*' => false,
                        ] as $columns => $expectSqlChange) {
                            
                            $sql = sprintf(
                                "SELECT %s FROM %s a %s %s b ON(a.%s = b.%s)",
                                $columns,
                                $aTable,
                                $joinType,
                                $bTable,
                                $aRefColumn,
                                $bRefColumn
                            );
                            
                            $tests[str_pad($counter, 3, '0', STR_PAD_LEFT) . ':' . $sql] = [$sql, $expectSqlChange];
                            $counter++;
                        }
                    }
                }
            }
        }

        return $tests;
    }

    private static function createSchema(): void
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
        
        self::$pdo->query('DROP TABLE IF EXISTS `customers`');
        self::$pdo->query('DROP TABLE IF EXISTS `sales`');
        self::$pdo->query('DROP TABLE IF EXISTS `sale_items`');
        self::$pdo->query('DROP TABLE IF EXISTS `articles`');
        self::$pdo->query('DROP TABLE IF EXISTS `ratings`');
        self::$pdo->query('DROP TABLE IF EXISTS `payments`');
        self::$pdo->query('DROP TABLE IF EXISTS `test1`');
        self::$pdo->query('DROP TABLE IF EXISTS `test2`');

        self::$pdo->query('
            CREATE TABLE `customers` (
                `id` VARCHAR(32) NOT NULL PRIMARY KEY,
                `name` VARCHAR(64) NOT NULL,
                `address` TINYTEXT
            );
        ');

        self::$pdo->query('
            CREATE TABLE `sales` (
                `id` VARCHAR(32) NOT NULL PRIMARY KEY,
                `invoice_number` VARCHAR(32) NOT NULL,
                `customer_id` VARCHAR(32) NOT NULL,
                `purchase_date` DATETIME NOT NULL
            );
        ');

        self::$pdo->query('
            CREATE TABLE `sale_items` (
                `id` VARCHAR(32) NOT NULL PRIMARY KEY,
                `sale_id` VARCHAR(32) NOT NULL,
                `article_id` VARCHAR(32) NOT NULL,
                `quantity` INT DEFAULT 1,
                `price` INT DEFAULT 0
            );
        ');

        self::$pdo->query('
            CREATE TABLE `ratings` (
                `id` VARCHAR(32) NOT NULL PRIMARY KEY,
                `sale_id` VARCHAR(32) NOT NULL UNIQUE,
                `rating` INT NOT NULL
            );
        ');

        self::$pdo->query('
            CREATE TABLE `articles` (
                `id` VARCHAR(32) NOT NULL PRIMARY KEY,
                `name` VARCHAR(64) NOT NULL,
                `purchase_price` INT NOT NULL,
                `selling_price` INT NOT NULL,
                `successed_by` VARCHAR(32) UNIQUE
            );
        ');

        self::$pdo->query('
            CREATE TABLE `payments` (
                `id` VARCHAR(32) NOT NULL PRIMARY KEY,
                `sale_id` VARCHAR(32),
                `paid_amount` INT NOT NULL,
                `reference` VARCHAR(64) NOT NULL
            );
        ');

        self::$pdo->query('
            CREATE TABLE `test1` (
                `id`                VARCHAR(32) NOT NULL PRIMARY KEY,
                `null_notunique`    VARCHAR(32),
                `notnull_notunique` VARCHAR(32) NOT NULL,
                `null_unique`       VARCHAR(32) UNIQUE
            );
        ');
        
        self::$pdo->query('
            CREATE TABLE `test2` (
                `id`                VARCHAR(32) NOT NULL PRIMARY KEY,
                `null_notunique`    VARCHAR(32),
                `notnull_notunique` VARCHAR(32) NOT NULL,
                `null_unique`       VARCHAR(32) UNIQUE,
                `notnull_unique`    VARCHAR(32) NOT NULL UNIQUE
            );
        ');
    }

    private static function insertTestFixtures(): void
    {
        self::label(true);

        /*** @var array<string, array<int, string>> */
        $ids = self::generateRowIds([
            'articles' => 5,
            'customers' => 2,
            'sales' => 4,
            'sale_items' => 8,
            'payments' => 4,
            'ratings' => 3,
            'test1' => 15,
            'test2' => 10,
        ]);

        self::$ids = $ids;

        self::$pdo->query('
            INSERT INTO `articles`
            (`id`, `name`, `purchase_price`, `selling_price`, `successed_by`)
            VALUES
            ("' . $ids['articles'][0] . '", "' . self::label() . '", 12300,  9900, NULL),
            ("' . $ids['articles'][1] . '", "' . self::label() . '", 12300, 12900, "' . $ids['articles'][0] . '"),
            ("' . $ids['articles'][2] . '", "' . self::label() . '", 12300,  4990, NULL),
            ("' . $ids['articles'][3] . '", "' . self::label() . '", 12300,  5500, "' . $ids['articles'][2] . '"),
            ("' . $ids['articles'][4] . '", "' . self::label() . '", 12300, 28850, NULL);
        ');

        self::$pdo->query('
            INSERT INTO `customers`
            (`id`, `name`, `address`)
            VALUES
            ("' . $ids['customers'][0] . '", "' . self::label() . '", "' . self::label() . '"),
            ("' . $ids['customers'][1] . '", "' . self::label() . '", NULL);
        ');

        self::$pdo->query('
            INSERT INTO `sales`
            (`id`, `invoice_number`, `customer_id`, `purchase_date`)
            VALUES
            ("' . $ids['sales'][0] . '", "' . self::label() . '", "' . $ids['customers'][0] . '", "' . self::date() . '"),
            ("' . $ids['sales'][1] . '", "' . self::label() . '", "' . $ids['customers'][0] . '", "' . self::date() . '"),
            ("' . $ids['sales'][2] . '", "' . self::label() . '", "' . $ids['customers'][1] . '", "' . self::date() . '"),
            ("' . $ids['sales'][3] . '", "' . self::label() . '", "' . $ids['customers'][1] . '", "' . self::date() . '");
        ');

        self::$pdo->query('
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

        self::$pdo->query('
            INSERT INTO `payments`
            (`id`, `sale_id`, `paid_amount`, `reference`)
            VALUES
            ("' . $ids['payments'][0] . '", "' . $ids['sales'][0] . '", 28373, "' . self::label() . '"),
            ("' . $ids['payments'][1] . '", "' . $ids['sales'][1] . '", 24632, "' . self::label() . '"),
            ("' . $ids['payments'][2] . '", "' . $ids['sales'][2] . '",  9743, "' . self::label() . '"),
            ("' . $ids['payments'][3] . '", "' . $ids['sales'][2] . '", 13879, "' . self::label() . '");
        ');

        self::$pdo->query('
            INSERT INTO `ratings`
            (`id`, `sale_id`, `rating`)
            VALUES
            ("' . $ids['ratings'][0] . '", "' . $ids['sales'][0] . '", 3),
            ("' . $ids['ratings'][1] . '", "' . $ids['sales'][1] . '", 1),
            ("' . $ids['ratings'][2] . '", "' . $ids['sales'][3] . '", 5);
        ');
        
        self::$pdo->query('
            INSERT INTO `test1`
            (                     `id`,          `null_notunique`,       `notnull_notunique`,             `null_unique`)
            VALUES
            ("' . $ids['test1'][0] . '",                       NULL, "' . $ids['test2'][1] . '",                       NULL),
            ("' . $ids['test1'][1] . '",                       NULL, "' . $ids['test2'][3] . '",                       NULL),
            ("' . $ids['test1'][2] . '", "' . $ids['test2'][1] . '", "' . $ids['test2'][1] . '",                       NULL),
            ("' . $ids['test1'][3] . '", "' . $ids['test2'][0] . '", "' . $ids['test2'][3] . '",                       NULL),
            ("' . $ids['test1'][4] . '", "' . $ids['test2'][0] . '", "' . $ids['test2'][3] . '",                       NULL),
            ("' . $ids['test1'][5] . '",                       NULL, "' . $ids['test2'][1] . '", "' . $ids['test2'][1] . '"),
            ("' . $ids['test1'][6] . '",                       NULL, "' . $ids['test2'][3] . '", "' . $ids['test2'][2] . '"),
            ("' . $ids['test1'][7] . '", "' . $ids['test2'][5] . '", "' . $ids['test2'][1] . '", "' . $ids['test2'][3] . '"),
            ("' . $ids['test1'][8] . '", "' . $ids['test2'][6] . '", "' . $ids['test2'][3] . '", "' . $ids['test2'][4] . '"),
            ("' . $ids['test1'][9] . '", "' . $ids['test2'][7] . '", "' . $ids['test2'][3] . '", "' . $ids['test2'][5] . '"),
            ("' . $ids['test1'][10]. '",                       NULL, "' . $ids['test2'][1] . '",                       NULL),
            ("' . $ids['test1'][11]. '",                       NULL, "' . $ids['test2'][3] . '",                       NULL),
            ("' . $ids['test1'][12]. '", "' . $ids['test2'][1] . '", "' . $ids['test2'][1] . '",                       NULL),
            ("' . $ids['test1'][13]. '", "' . $ids['test2'][0] . '", "' . $ids['test2'][3] . '",                       NULL),
            ("' . $ids['test1'][14]. '", "' . $ids['test2'][0] . '", "' . $ids['test2'][3] . '",                       NULL);
        ');
        
        self::$pdo->query('
            INSERT INTO `test2`
            (                     `id`,          `null_notunique`,       `notnull_notunique`,             `null_unique`,          `notnull_unique`)
            VALUES
            ("' . $ids['test2'][0] . '",                       NULL, "' . $ids['test1'][1] . '",                       NULL, "' . $ids['test1'][1] . '"),
            ("' . $ids['test2'][1] . '",                       NULL, "' . $ids['test1'][3] . '",                       NULL, "' . $ids['test1'][2] . '"),
            ("' . $ids['test2'][2] . '", "' . $ids['test1'][1] . '", "' . $ids['test1'][1] . '",                       NULL, "' . $ids['test1'][3] . '"),
            ("' . $ids['test2'][3] . '", "' . $ids['test1'][0] . '", "' . $ids['test1'][3] . '",                       NULL, "' . $ids['test1'][4] . '"),
            ("' . $ids['test2'][4] . '", "' . $ids['test1'][0] . '", "' . $ids['test1'][3] . '",                       NULL, "' . $ids['test1'][5] . '"),
            ("' . $ids['test2'][5] . '",                       NULL, "' . $ids['test1'][1] . '", "' . $ids['test1'][1] . '", "' . $ids['test1'][6] . '"),
            ("' . $ids['test2'][6] . '",                       NULL, "' . $ids['test1'][3] . '", "' . $ids['test1'][2] . '", "' . $ids['test1'][7] . '"),
            ("' . $ids['test2'][7] . '", "' . $ids['test1'][5] . '", "' . $ids['test1'][1] . '", "' . $ids['test1'][3] . '", "' . $ids['test1'][8] . '"),
            ("' . $ids['test2'][8] . '", "' . $ids['test1'][6] . '", "' . $ids['test1'][3] . '", "' . $ids['test1'][4] . '", "' . $ids['test1'][9] . '"),
            ("' . $ids['test2'][9] . '", "' . $ids['test1'][7] . '", "' . $ids['test1'][3] . '", "' . $ids['test1'][5] . '", "' . $ids['test1'][0] . '");
        ');
    }

    /*** @return array<string, array<int, string>> */
    private static function generateRowIds(array $tableDef): array
    {
        return array_combine(
            array_keys($tableDef),
            array_map(function (int $idCount): array {
                return array_map(function (): string {
                    return self::label(6);
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
