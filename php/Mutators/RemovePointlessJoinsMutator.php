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
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstSelect;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstJoin;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstOrderBy;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstWhere;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstColumn;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstOperation;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstLiteral;
use Addiks\StoredSQL\Lexing\SqlToken;
use Webmozart\Assert\Assert;
use Addiks\StoredSQL\Schema\Schemas;
use Addiks\StoredSQL\Schema\Column;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstFrom;
use Addiks\StoredSQL\Schema\Table;

/** @psalm-import-type Mutator from SqlAstMutableNode */
final class RemovePointlessJoinsMutator
{
    /** @return Mutator */
    public static function create(): Closure
    {
        return Closure::fromCallable([
            new RemovePointlessJoinsMutator(), 
            'removePointlessJoins'
        ]);
    }
    
    public function removePointlessJoins(
        SqlAstNode $node, 
        int $offset, 
        SqlAstMutableNode $parent,
        Schemas $schemas
    ): void {
        if ($node instanceof SqlAstSelect) {
            /** @var SqlAstSelect $select */
            $select = $node;
            
            /** @var ExecutionContext $context */
            $context = $select->createContext($schemas);
            
            foreach ($select->joins() as $join) {
                if (!$this->isJoinAliasUsedInSelect($join, $select)
                 && !$this->canJoinChangeResultSetSize($join, $select, $context)) {
                    $select->replaceJoin($join, null);
                }
            }
        }
    }
    
    private function isJoinAliasUsedInSelect(SqlAstJoin $join, SqlAstSelect $select): bool
    {
        /** @var bool $isJoinAliasUsedInSelect */
        $isJoinAliasUsedInSelect = false;
        
        /** @var string $joinName */
        $joinName = ($join->alias() ?? $join->joinedTable)->toSql();
        
        /** @var SqlAstNode $selectChildNode */
        foreach ($select->children() as $selectChildNode) {
            if ($selectChildNode === $join) {
                continue;
            }
            
            $selectChildNode->walk(function (SqlAstNode $node) use (&$isJoinAliasUsedInSelect, $joinName): void {
                
                if ($node instanceof SqlAstColumn) {
                    if ($node->tableName()->toSql() === $joinName) {
                        $isJoinAliasUsedInSelect = true;
                    }
                }
                
            });
        }    
        
        return $isJoinAliasUsedInSelect;
    }
    
    private function canJoinChangeResultSetSize(
        SqlAstJoin $join, 
        SqlAstSelect $select,
        ExecutionContext $context
    ): bool {
        /** @var bool $canJoinChangeResultSetSize */
        $canJoinChangeResultSetSize = true;
        
        /** @var SqlAstExpression|null $condition */
        $condition = $join->condition();
        
        if ($join->isUsingColumnCondition()) {
            # "... JOIN foo USING(bar_id)"
            
            if ($condition instanceof SqlAstColumn) {
                return $this->canUsingJoinChangeResultSetSize($join, $context);
            }
            
        } elseif (is_object($condition)) {
            # "... JOIN foo ON(foo.id = bar.foo_id)"
            
            return $this->canOnJoinChangeResultSetSize($join, $context);
        }
        
        return true;
    }
    
    private function canUsingJoinChangeResultSetSize(SqlAstJoin $join, ExecutionContext $context): bool
    {
        /** @var SqlAstExpression|null $column */
        $column = $join->condition();
        
        Assert::isInstanceOf($column, SqlAstColumn::class);
        
        /** @var string $columnName */
        $columnName = $column->columnNameString();
        
        return !$context->isOneToOneRelation(
            $context->findTableWithColumn($columnName),
            $columnName, 
            $join->joinedTable(),
            $columnName
        );
    }
    
    private function canOnJoinChangeResultSetSize(SqlAstJoin $join, ExecutionContext $context): bool
    {
        /** @var SqlAstExpression|null $condition */
        $condition = $join->condition();
        
        /** @var array<SqlAstOperation> $equations */
        $equations = $condition->extractFundamentalEquations();
        
        /** @var array<SqlAstOperation> $alwaysFalseEquations */
        $alwaysFalseEquations = array_filter($equations, function (SqlAstOperation $equation): bool {
            return $equation->isAlwaysFalse();
        });
        
        if (!empty($alwaysFalseEquations)) {
            return true;
        }
        
        $equations = array_filter($equations, function (SqlAstOperation $equation): bool {
            return !$equation->isAlwaysTrue();
        });
        
        /** @var SqlAstOperation */
        foreach ($equations as $equation) {
            if (!$context->isEquationOneOnOneRelation($equation)) {
                return true;
            }
        }
        
        return false;
    }
}
