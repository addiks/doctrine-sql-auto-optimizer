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
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstFunctionCall;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstGroupBy;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstJoin;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstMutableNode;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstNode;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstSelect;
use Addiks\StoredSQL\ExecutionContext;
use Addiks\StoredSQL\Schema\Column;
use Addiks\StoredSQL\Schema\Schemas;
use Closure;

/** @psalm-import-type Mutator from SqlAstMutableNode */
final class RemovePointlessGroupByMutator
{
    /** @return Mutator */
    public static function create(): Closure
    {
        return Closure::fromCallable([
            new RemovePointlessGroupByMutator(),
            'removePointlessGroupBy',
        ]);
    }

    public function removePointlessGroupBy(
        SqlAstNode $node,
        int $offset,
        SqlAstMutableNode $parent,
        Schemas $schemas
    ): void {
        if ($node instanceof SqlAstSelect) {
            /** @var SqlAstSelect $select */
            $select = $node;

            /** @var SqlAstGroupBy|null $groupBy */
            $groupBy = $select->groupBy();

            if (is_object($groupBy)) {
                /** @var ExecutionContext $context */
                $context = $select->createContext($schemas);

                if ($this->isUniqueGroupedExpression($groupBy->expression(), $context)
                 && $this->hasOnlyOneToOneJoins($select, $context)
                 && !$this->usesAggregatingFunctions($select)) {
                    $select->replaceGroupBy(null);
                }
            }
        }
    }

    private function isUniqueGroupedExpression(
        SqlAstExpression $expression,
        ExecutionContext $context
    ): bool {
        if ($expression instanceof SqlAstColumn) {
            /** @var Column|null $column */
            $column = $context->columnByNode($expression);

            if ($column->unique()) {
                return true;
            }
        }

        return false;
    }

    private function hasOnlyOneToOneJoins(
        SqlAstSelect $select,
        ExecutionContext $context
    ): bool {
        /** @var bool $hasOnlyOneToOneJoins */
        $hasOnlyOneToOneJoins = true;

        /** @var SqlAstJoin $join */
        foreach ($select->joins() as $join) {
            if ($join->canChangeResultSetSize($context)) {
                $hasOnlyOneToOneJoins = false;
                break;
            }
        }

        return $hasOnlyOneToOneJoins;
    }

    private function usesAggregatingFunctions(SqlAstSelect $select): bool
    {
        /** @var bool $usesAggregatingFunctions */
        $usesAggregatingFunctions = false;

        # Following list according to https://dev.mysql.com/doc/refman/8.0/en/aggregate-functions.html

        /** @var array<string> $aggregatingFunctions */
        $aggregatingFunctions = array(
            'AVG',
            'BIT_AND',
            'BIT_OR',
            'BIT_XOR',
            'COUNT',
            'COUNT',
            'GROUP_CONCAT',
            'JSON_ARRAYAGG',
            'JSON_OBJECTAGG',
            'MAX',
            'MIN',
            'STD',
            'STDDEV',
            'STDDEV_POP',
            'STDDEV_SAMP',
            'SUM',
            'VAR_POP',
            'VAR_SAMP',
            'VARIANCE',
        );

        $select->walk([function (SqlAstNode $node) use (&$usesAggregatingFunctions, $aggregatingFunctions): void {
            if ($node instanceof SqlAstFunctionCall) {
                if (in_array($node->toSql(), $aggregatingFunctions, true)) {
                    $usesAggregatingFunctions = true;
                }
            }
        }]);

        return $usesAggregatingFunctions;
    }
}
