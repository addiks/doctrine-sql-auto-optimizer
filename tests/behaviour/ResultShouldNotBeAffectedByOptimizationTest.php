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
use Psr\SimpleCache\CacheInterface;
use PDO;
use PDOStatement;

final class ResultShouldNotBeAffectedByOptimizationTest extends TestCase
{

    private DefaultSQLOptimizer $optimizer;

    private CacheInterface $cache;
    
    private PDO $pdo;

    public function setUpBeforeClass(): void
    {
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
        
        $this->pdo = new PDO('sqlite::memory:');
        
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
        
        /** @var string $currentLabel */
        $currentLabel = md5('');
        
        /** @var array<int, string> $labels */
        $labels = array();
        
        $label = function (int $length = 8) use (&$currentLabel, &$labels): string
        {
            $currentLabel = md5($currentLabel);
            
            /** @var string $newLabel */
            $newLabel = substr($currentLabel, 0, $length);
            
            array_unshift($labels, $newLabel);
            
            return $newLabel
        }
        
        $this->pdo->query('
            INSERT INTO `articles` 
            (`id`, `name`, `purchase_price`, `selling_price`, `successed_by`)
            VALUES 
            ('.$label(32).', '.$label().',  9900, NULL),
            ('.$label(32).', '.$label().', 12900, '.$labels[3].'),
            ('.$label(32).', '.$label().',  4990, NULL),
            ('.$label(32).', '.$label().',  5500, '.$labels[3].'),
            ('.$label(32).', '.$label().', 28850, NULL);
        ');
        
        /** @var array<int, string> $articleIds */
        $articleIds = [$labels[1], $labels[5], $labels[7], $labels[10], $labels[12]];
        
        $this->pdo->query('
            INSERT INTO `customers`
            (`id`, `name`, `address`)
            VALUES
            ('.$label(32).', '.$label(8).', '.$label(32).'),
            ('.$label(32).', '.$label(8).');
        ');
    }
    
    /** 
     * @test 
     * @dataProvider generateTestData
     */
    public function resultShouldNotBeAffectedByOptimization(string $originalSql): void
    {
        /** @var string $optimizedSql */
        $optimizedSql = $this->optimizer->optimizeSql($originalSql);
        
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
        
        $this->assertEquals($expectedResult, $actualResult);
    }
    
    public function generateTestData(): array
    {
        /** @var array<array{0: string}> $tests */
        $tests = array();
        
        return $tests;
    }

}
