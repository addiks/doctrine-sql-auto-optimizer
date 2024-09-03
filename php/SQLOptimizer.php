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

use Addiks\StoredSQL\Schema\Schemas;

/**
 * @psalm-type QueryOptimizedListener = callable(string, string): void
 */
interface SQLOptimizer
{
    public function optimizeSql(string $inputSql, Schemas $schemas): string;

    /** @param QueryOptimizedListener $listener */
    public function addQueryOptimizedListener(callable $listener): void;

    public function warmUpCacheFromSqlLog(Schemas $schemas): void;
}
