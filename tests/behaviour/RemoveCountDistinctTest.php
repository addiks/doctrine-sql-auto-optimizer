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

final class RemoveCountDistinctTest extends TestCase
{
    /** @test */
    public function shouldRemoveCountDistinct(): void
    {
        $optimizer = new DefaultSQLOptimizer();
        $schemas = new SchemasClass();
        $schema = new SchemaClass($schemas, 'sample_schema');
        $table = new TableClass($schema, 'production_order');
        new ColumnClass($table, 'id', SqlTypeClass::fromName('INT'), false, true);
        
        $originalSql = 'SELECT COUNT(DISTINCT p0_.id) AS sclr_0 FROM production_order p0_';
    
        $optimizedSql = $optimizer->optimizeSql($originalSql, $schemas);
        
        $this->assertSame(
            'SELECT COUNT(p0_.id) AS sclr_0 FROM production_order p0_',
            $optimizedSql
        );
    }
}
