<?php

namespace Addiks\StoredSQL;

use Addiks\MorePhpCsFixers\Whitespace\BlankLineBeforeCatchBlockFixer;
use Addiks\MorePhpCsFixers\Whitespace\BlankLineBeforeDocCommentFixer;
use Addiks\MorePhpCsFixers\Whitespace\BlankLineBeforeElseBlockFixer;
use PhpCsFixer\Config;
use PhpCsFixer\Finder;
use PhpCsFixerCustomFixers\Fixers;
use Symfony\Component\Yaml\Yaml;

require_once ('vendor/autoload.php');

$finder = Finder::create()
    ->in(__DIR__)
    ->exclude(['var', 'vendor']);

$rules = Yaml::parseFile(__DIR__ . '/php-cs-fixer.yml');

/** @var Config $config */
$config = new Config();
$config->registerCustomFixers([
    new BlankLineBeforeCatchBlockFixer(),
    new BlankLineBeforeElseBlockFixer(),
    new BlankLineBeforeDocCommentFixer(),
]);
$config->registerCustomFixers(new Fixers());
$config->setRules($rules);
$config->setFinder($finder);
$config->setRiskyAllowed(true);

return $config;
