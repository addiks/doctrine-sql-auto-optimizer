# Doctrine SQL Auto-Optimizer

This is a drop-in zero-configuration Doctrine extension that optimizes all SQL queries before execution.
(It can also be used independently of doctrine, see below ...)

All of these optimizations can (depending on context) greatly improve the execution-speed of the executed SQL statements.

## Currently implemented:

* Removes JOIN's when they are not referenced anywhere else in the query and cannot have an impact on the result-set
  size.
* Removes GROUP BY statements when all JOIN's are one-to-one and the grouping expression is a unique column.
* Removes DISTINCT from a SELECT statement if there cannot be any duplicated rows in the result-set.
* Removes DISTINCT from a COUNT(DISTINCT ...) if all counted values already are distinct.

## Setup

First: `composer require addiks/doctrine-sql-auto-optimizer`

Then, depending on your system, there are multiple ways to activate the extension:

### Option 1) Symfony

You can either import the services-xml file that is bundles with this package:

```xml
<imports>
    <import resource="../../vendor/addiks/doctrine-sql-auto-optimizer/symfony-services.xml" />
</imports>
```
(You may need to alter the import path, depending on your configuration)

Or you can define your own service:

```xml
<service
    id="addiks_auto_optimizer.doctrine.event_listener"
    class="Addiks\DoctrineSqlAutoOptimizer\DoctrineEventListener"
>
    <argument type="service" id="logger" />
    <argument type="service" id="cache.app.simple" />
    <tag name="doctrine.event_listener" event="postConnect" />
</service>
```

### Option 2) Doctrine

Make sure the following is executed before doctrine connects to the database:

```php
$sqlOptimizingEventListener = new \Addiks\DoctrineSqlAutoOptimizer\DoctrineEventListener(
    $logger, # Monolog\Logger                  REQUIRED
    $cache   # Psr\SimpleCache\CacheInterface  OPTIONAL
);

# Doctrine\Common\EventManager
$eventManager->addEventListener(['postConnect'], $sqlOptimizingEventListener);
```

The (monolog-) logger is required so that the optimizer can report any issues to you (as notices).

The cache is optional, but highly recommended. Without cache, the (slow) optimizing process runs for every single query.

### Option 3) Native PHP (without doctrine)

You can also use the query optimizer completely without doctrine:

```php
$schemas = \Addiks\StoredSQL\Schema\SchemasClass::fromPDO(
    $pdo,   # \PDO                            REQUIRED
    $cache, # Psr\SimpleCache\CacheInterface  OPTIONAL
);

$optimizer = new \Addiks\DoctrineSqlAutoOptimizer\DefaultSQLOptimizer(
    $cache   # Psr\SimpleCache\CacheInterface  OPTIONAL
);

$optimizedSql = $optimizer->optimizeSql($inputSql, $schemas);
```

Again, the cache is optional but highly recommended. No cache, no speed.
The creation of the schema in the `SchemasClass::fromPDO` call is cached (if you provide a cache, that is).
