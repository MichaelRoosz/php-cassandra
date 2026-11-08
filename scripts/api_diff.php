<?php

declare(strict_types=1);

/**
 * Lists how the public API changed since a tag, so that the changelog's breaking
 * changes can be checked against a fact rather than against memory.
 * `@internal` classes are left out.
 *
 * Usage: php scripts/api_diff.php <git-tag>
 */

$tag = $argv[1] ?? '';
if ($tag === '') {
    fwrite(STDERR, "Usage: php scripts/api_diff.php <git-tag>\n");

    exit(2);
}

/**
 * @return array<string, string> class member => signature
 *
 * @throws \TypeError
 * @throws \UnexpectedValueException
 */
function dumpApi(string $source): array {
    $api = [];
    $directory = new RecursiveDirectoryIterator($source, FilesystemIterator::SKIP_DOTS);
    $files = [];
    /** @var SplFileInfo $file */
    foreach (new RecursiveIteratorIterator($directory) as $file) {
        if ($file->getExtension() === 'php') {
            $files[] = $file->getPathname();
        }
    }

    spl_autoload_register(static function (string $class) use ($source): void {
        if (str_starts_with($class, 'Cassandra\\')) {
            $path = $source . '/' . str_replace('\\', '/', substr($class, strlen('Cassandra\\'))) . '.php';
            if (is_file($path)) {
                require_once $path;
            }
        }
    });

    sort($files);
    foreach ($files as $file) {
        $class = 'Cassandra\\' . str_replace(['/', '.php'], ['\\', ''], substr($file, strlen($source) + 1));
        if (!class_exists($class) && !interface_exists($class) && !enum_exists($class)) {
            continue;
        }

        $reflection = new ReflectionClass($class);
        $docComment = (string) $reflection->getDocComment();
        if (str_contains($docComment, '@internal')) {
            continue;
        }

        $kind = $reflection->isInterface() ? 'interface' : ($reflection->isEnum() ? 'enum' : 'class');
        $modifiers = ($reflection->isFinal() && !$reflection->isEnum() ? 'final ' : '') . ($reflection->isAbstract() && !$reflection->isInterface() ? 'abstract ' : '');
        $parent = $reflection->getParentClass();
        $api[$class] = $modifiers . $kind . ($parent !== false ? ' extends ' . $parent->getName() : '');

        foreach ($reflection->getReflectionConstants(ReflectionClassConstant::IS_PUBLIC) as $constant) {
            if ($constant->getDeclaringClass()->getName() === $class) {
                $api[$class . '::' . $constant->getName()] = $constant->isEnumCase() ? 'case' : 'const';
            }
        }

        foreach ($reflection->getProperties(ReflectionProperty::IS_PUBLIC) as $property) {
            if ($property->getDeclaringClass()->getName() === $class || $property->isPromoted()) {
                $api[$class . '::$' . $property->getName()] = (string) $property->getType();
            }
        }

        foreach ($reflection->getMethods(ReflectionMethod::IS_PUBLIC | ReflectionMethod::IS_PROTECTED) as $method) {
            if ($method->getDeclaringClass()->getName() !== $class) {
                continue;
            }

            $parameters = [];
            foreach ($method->getParameters() as $parameter) {
                $parameters[] = trim($parameter->getType() . ' $' . $parameter->getName())
                    . ($parameter->isOptional() ? '?' : '');
            }

            $api[$class . '::' . $method->getName() . '()'] = ($method->isProtected() ? 'protected ' : '')
                . ($method->isStatic() ? 'static ' : '')
                . '(' . implode(', ', $parameters) . '): ' . $method->getReturnType();
        }
    }

    return $api;
}

if (($argv[2] ?? '') === '--dump') {
    echo json_encode(dumpApi($argv[3]), JSON_THROW_ON_ERROR);

    exit(0);
}

$root = dirname(__DIR__);
$temporary = sys_get_temp_dir() . '/php-cassandra-api-diff-' . getmypid();
mkdir($temporary);

$archive = $temporary . '/source.tar';
exec(sprintf('git -C %s archive --format=tar -o %s %s src', escapeshellarg($root), escapeshellarg($archive), escapeshellarg($tag)), result_code: $archiveResult);
if ($archiveResult !== 0) {
    fwrite(STDERR, "Cannot read tag {$tag}\n");

    exit(2);
}
exec(sprintf('tar -xf %s -C %s', escapeshellarg($archive), escapeshellarg($temporary)));

// Two processes, since both versions declare the same classes.
$dump = static function (string $source): array {
    $output = shell_exec(sprintf(
        '%s %s dump --dump %s',
        escapeshellarg(PHP_BINARY),
        escapeshellarg(__FILE__),
        escapeshellarg($source)
    ));

    /** @var array<string, string> */
    return json_decode((string) $output, true, flags: JSON_THROW_ON_ERROR);
};

$old = $dump($temporary . '/src');
$new = $dump($root . '/src');

exec('rm -rf ' . escapeshellarg($temporary));

$removed = array_diff_key($old, $new);
$changed = array_filter(array_intersect_key($old, $new), static fn (string $signature, string $name): bool => $new[$name] !== $signature, ARRAY_FILTER_USE_BOTH);
$added = array_diff_key($new, $old);

echo "=== Removed since {$tag} (" . count($removed) . ") ===\n";
foreach ($removed as $name => $signature) {
    echo "- {$name}  {$signature}\n";
}

echo "\n=== Changed since {$tag} (" . count($changed) . ") ===\n";
foreach ($changed as $name => $signature) {
    echo "~ {$name}\n    was {$signature}\n    now {$new[$name]}\n";
}

echo "\n=== Added since {$tag}: " . count($added) . " members (not listed) ===\n";
