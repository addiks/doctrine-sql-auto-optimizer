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

use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstAllColumnsSelector;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstColumn;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstJoin;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstMutableNode;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstNode;
use Addiks\StoredSQL\AbstractSyntaxTree\SqlAstSelect;
use Addiks\StoredSQL\ExecutionContext;
use Addiks\StoredSQL\Schema\Schemas;
use Closure;

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
                 && !$join->canChangeResultSetSize($context)) {
                    $select->replaceJoin($join, null);
                }
            }
        }
    }

    private function isJoinAliasUsedInSelect(SqlAstJoin $join, SqlAstSelect $select): bool
    {
        /** @var bool $isJoinAliasUsedInSelect */
        $isJoinAliasUsedInSelect = false;

        /** @var SqlAstNode $selectChildNode */
        foreach ($select->children() as $selectChildNode) {
            if ($selectChildNode === $join) {
                continue;
            }

            $selectChildNode->walk([function (SqlAstNode $node) use (&$isJoinAliasUsedInSelect, $join): void {
                if ($node instanceof SqlAstColumn) {
                    if ($node->tableNameString() === $join->aliasName()) {
                        $isJoinAliasUsedInSelect = true;
                    }

                } elseif ($node instanceof SqlAstAllColumnsSelector) {
                    if (empty($node->tableNameString())) {
                        $isJoinAliasUsedInSelect = true;

                    } elseif ($node->tableNameString() === $join->aliasName()) {
                        if (empty($node->schemaNameString())) {
                            $isJoinAliasUsedInSelect = true;

                        } elseif ($node->schemaNameString() === $join->schemaName()) {
                            $isJoinAliasUsedInSelect = true;
                        }
                    }
                }
            }]);
        }

        return $isJoinAliasUsedInSelect;
    }
}
