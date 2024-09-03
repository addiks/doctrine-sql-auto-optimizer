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

namespace Addiks\DoctrineSqlAutoOptimizer\Mutators;

use Closure;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstMutableNode;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstNode;
use Addiks\StoredSQL\Schema\Schemas;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstTokenNode;
use Addiks\StoredSQL\Lexing\SqlToken;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstSelect;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstAllColumnsSelector;
use Addiks\StoredSQL\Schema\Table;
use Addiks\StoredSQL\Schema\Column;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstFrom;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstJoin;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstTable;
use Addiks\StoredSQL\Schema\Schema;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstExpression;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstColumn;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstFunctionCall;
use Addiks\StoredSQL\ExecutionContext;
use Webmozart\Assert\Assert;

/** @psalm-import-type Mutator from SqlAstMutableNode */
final class CountDistinctRemover
{

    /** @return Mutator */
    public static function create(): Closure
    {
        return Closure::fromCallable([
            new CountDistinctRemover(),
            'removePointlessDistinct',
        ]);
    }
    
    public function removePointlessDistinct(
        SqlAstNode $node,
        int $offset,
        SqlAstMutableNode $parent,
        Schemas $schemas
    ): void {
        if ($node instanceof SqlAstSelect) {
            /** @var SqlAstSelect $select */
            $select = $node;
            
            /** @var SqlAstExpression|SqlAstAllColumnsSelector $column */
            foreach ($select->columns() as $column) {
                if ($column instanceof SqlAstFunctionCall && strtoupper($column->name()) === 'COUNT') {
                    $this->processFunctionCallNode($select, $column, $schemas);
                }
            }
        }
    }
        
    private function processFunctionCallNode(
        SqlAstSelect $select,
        SqlAstFunctionCall $count, 
        Schemas $schemas
    ): void {
        /** @var array<int, SqlAstExpression|SqlAstAllColumnsSelector> $arguments */
        $arguments = $count->arguments();
        
        if (count($arguments) !== 1) {
            return;
        }
        
        /** @var SqlAstTokenNode|null $distinct */
        $distinct = array_filter($count->flags(), fn($f) => $f->isCode('DISTINCT'))[0] ?? null;
        
        if (!$arguments[0] instanceof SqlAstColumn || is_null($distinct)) {
            return;
        }
        
        if (!isset($GLOBALS['__ADDIKS_DEBUG_IGNORE_COUNT_DISTINCT_REMOVAL_CHECK'])) {
            /** @var ExecutionContext $context */
            $context = $select->createContext($schemas);
                
            /** @var bool $hasToManyJoins */
            $hasToManyJoins = false;
            
            /** @var SqlAstJoin $join */
            foreach ($select->joins() as $join) {
                if ($join->canChangeResultSetSize($context)) {
                    return;
                }
            }        
            
            if ($hasToManyJoins) {
                return;
            }       
            
            /** @var SqlAstColumn $sqlColumn */
            $sqlColumn = $arguments[0]; 

            /** @var Column|null $column */
            $column = $context->columnByNode($sqlColumn);
            
            if (is_null($column) || !$column->unique() || $column->nullable()) {
                return;
            }
        }

        $count->removeFlag($distinct);
    }
}
