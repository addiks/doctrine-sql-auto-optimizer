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
use Addiks\StoredSQL\ExecutionContext;

/** @psalm-import-type Mutator from SqlAstMutableNode */
final class SelectDistinctRemover
{

    /** @return Mutator */
    public static function create(): Closure
    {
        return Closure::fromCallable([
            new SelectDistinctRemover(),
            'removePointlessDistinct',
        ]);
    }
    
    public function removePointlessDistinct(
        SqlAstNode $node,
        int $offset,
        SqlAstMutableNode $parent,
        Schemas $schemas
    ): void {
        if ($node instanceof SqlAstSelect && $node->isDistinct()) {
            /** @var SqlAstSelect $select */
            $select = $node;
            
            /** @var ExecutionContext $context */
            $context = $node->createContext($schemas);
            
            if ($this->doesSelectHaveAJoinThatCanIncreaseResultSet($select, $schemas, $context)) {
                # In to-many JOIN's, even UNIQUE columns can appear multiply times
                return;
            }
                 
            # If any of the selected columns is truly UNIQUE(*), then there can't be any duplicate rows in the result.
            # If there can't be any duplicate rows in the result, DISTINCT is pointless and only reduces performance.
            #
            # (* A UNIQUE column in a to-many joined table will not be considered truly UNIQUE in this case.)
            if ($this->doesSelectContainUniqueColumn($select, $schemas, $context)) {
                $select->removeDistinct();
            }
        }
    }
    
    private function doesSelectContainUniqueColumn(
        SqlAstSelect $select, 
        Schemas $schemas, 
        ExecutionContext $context
    ): bool {
        /** @var bool $hasUniqueColumn */
        $hasUniqueColumn = false;
               
        /** @var SqlAstExpression|SqlAstAllColumnsSelector $column */
        foreach ($select->columns() as $column) {
            if ($column instanceof SqlAstAllColumnsSelector) {
                /** @var Schema|null $schema */
                $schema = $schemas->schema($column->schemaNameString() ?? '') ?? $schemas->defaultSchema();
                
                if (is_null($schema)) {
                    return false; # We don't know the current used schema, might be unsafe to continue
                }
                
                /** @var string|null $tableName */
                $tableName = $column->tableNameString();
                
                if (empty($tableName)) {
                    # Columns selects all columns from _ALL_ selected source tables (SELECT * FROM ...)
                    
                    if ($this->doesSelectSourceTablesContainUniqueColumn($select, $schemas)) {
                        $hasUniqueColumn = true;
                        break;
                    }
                    
                } else {
                    # Columns selects all columns from _ONE_ selected source table (SELECT foo.* FROM ...)
                    
                    /** @var Table|null $table */
                    $table = $schema->table($tableName);
                    
                    if (is_null($table)) {
                        return false; # Unknown table (some kind of invalid statement?), might be unsafe to continue
                    }
                    
                    if ($this->doesTableContainUniqueColumn($table)) {
                        $hasUniqueColumn = true;
                        break;
                    }
                }
                
            } elseif ($column instanceof SqlAstColumn) {
                /** @var Table|null $table */
                $table = $this->findTableInSchemas(
                    $schemas, 
                    $column->schemaNameString(), 
                    $column->tableNameString()
                );
                
                if (is_null($table)) {
                    return false; # Unsafe => Do not modify SQL
                }
                
            } else {
                return false; # More complex column, do not modify SQL
            }
        }
        
        return $hasUniqueColumn;
    }
    
    private function doesSelectHaveAJoinThatCanIncreaseResultSet(
        SqlAstSelect $select, 
        Schemas $schemas, 
        ExecutionContext $context
    ): bool {
        /** @var SqlAstJoin $join */
        foreach ($select->joins() as $join) {
            if ($join->canChangeResultSetSize($context)) { # TODO: Not all size-changes are increases
                return true;
            }
        }
            
        return false;
    }
        
    private function doesSelectSourceTablesContainUniqueColumn(SqlAstSelect $select, Schemas $schemas): bool
    {
        /** @var array<int, SqlAstTable> $tables */
        $tables = array();
        
        /** @var SqlAstFrom|null $from */
        $from = $select->from();
        
        if (is_object($from)) {
            $tables[] = $from->table();
        }
        
        /** @var SqlAstJoin $join */
        foreach ($select->joins() as $join) {
            $tables[] = $join->joinedTable();
        }
        
        /** @var SqlAstTable $tableName */
        foreach ($tables as $tableName) {
            /** @var Table|null $table */
            $table = $this->findTableInSchemas($schemas, $tableName->schemaName(), $tableName->tableName());
            
            if (is_null($table)) {
                return true; # Unsafe => Do not modify SQL
            }
            
            if ($this->doesTableContainUniqueColumn($table)) {
                return true;
            }
        }
        
        return false;
    }
    
    private function doesTableContainUniqueColumn(Table $table): bool
    {
        /** @var Column $column */
        foreach ($table->columns() as $column) {
            if ($column->unique()) {
                return true;
            }
        }
        
        return false;
    }
    
    private function findTableInSchemas(Schemas $schemas, string|null $schemaName, string $tableName): ?Table
    {
        /** @var Schema|null $schema */
        $schema = $schemas->schema($schemaName ?? '') ?? $schemas->defaultSchema();
        
        if (is_null($schema)) {
            return null;
        }
        
        /** @var Table|null $table */
        $table = $schema->table($tableName);
        
        return $table;
    }

}
