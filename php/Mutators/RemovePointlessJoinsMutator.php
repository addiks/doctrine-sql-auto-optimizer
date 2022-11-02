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

namespace Addiks\DoctrineSqlAutoOptimizer\Mutators;

use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstColumn;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstExpression;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstJoin;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstMutableNode;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstNode;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstOperation;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstSelect;
use Addiks\StoredSQL\ExecutionContext;
use Addiks\StoredSQL\Schema\Column;
use Addiks\StoredSQL\Schema\Schemas;
use Closure;
use Webmozart\Assert\Assert;

/** @psalm-import-type Mutator from SqlAstMutableNode */
final class RemovePointlessJoinsMutator
{
    /** @return Mutator */
    public static function create(): Closure
    {
        return Closure::fromCallable([
            new RemovePointlessJoinsMutator(),
            'removePointlessJoins',
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

            if (empty($select->joins())) {
                return;
            }

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
        $joinName = ($join->alias() ?? $join->joinedTable())->toSql();

        /** @var SqlAstNode $selectChildNode */
        foreach ($select->children() as $selectChildNode) {
            if ($selectChildNode === $join) {
                continue;
            }

            $selectChildNode->walk([function (SqlAstNode $node) use (&$isJoinAliasUsedInSelect, $joinName): void {
                if ($node instanceof SqlAstColumn) {
                    if ($node->tableNameString() === $joinName) {
                        $isJoinAliasUsedInSelect = true;
                    }
                }
            }]);
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

        /** @var string $joinAlias */
        $joinAlias = $join->aliasName();

        foreach ($equations as $equation) {
            /** @var SqlAstExpression $leftSide */
            $leftSide = $equation->leftSide();

            /** @var SqlAstExpression $rightSide */
            $rightSide = $equation->rightSide();

            if ($leftSide instanceof SqlAstColumn && $leftSide->tableNameString() === $joinAlias) {
                /** @var SqlAstExpression $joinedSide */
                $joinedSide = $leftSide;

            } elseif ($rightSide instanceof SqlAstColumn && $rightSide->tableNameString() === $joinAlias) {
                /** @var SqlAstExpression $joinedSide */
                $joinedSide = $rightSide;

            } else {
                # Unknown condition, let's assume that this JOIN can change result size to be safe.
                return true;
            }

            if ($joinedSide instanceof SqlAstColumn) {
                /** @var Column|null $joinedColumn */
                $joinedColumn = $context->columnByNode($joinedSide);

                if ($join->isOuterJoin()) {
                    return !$joinedColumn->unique();

                } else {
                    return !$joinedColumn->unique() || $joinedColumn->nullable();
                }

            } else {
                # Either a literal (which will change result size), or an unknown condition (which might change it).
                return true;
            }
        }

        # All equations are either always true or always false. Either way, this JOIN changes the result size.
        return true;
    }
}
