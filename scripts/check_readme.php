<?php

declare(strict_types=1);

/**
 * Checks README.md against the code, so that its examples cannot quietly rot:
 *
 * - every PHP block parses;
 * - every imported class, instantiated class, static member, Connection method
 *   and named argument exists;
 * - every internal link has a heading to land on;
 * - every block that needs no connection runs without an exception.
 *
 * Usage: php scripts/check_readme.php [file]
 */

require __DIR__ . '/../vendor/autoload.php';

use Cassandra\Connection;
use Cassandra\Statement;

$file = $argv[1] ?? dirname(__DIR__) . '/README.md';
$readme = (string) file_get_contents($file);
$errors = [];

// Names only ever used in the migration examples of other drivers.
const FOREIGN_NAMESPACE = 'Cassandra::';
const FOREIGN_CLASSES = ['Cassandra\\SimpleStatement', 'Cassandra\\Uuid', 'Cassandra\\Timestamp'];

/**
 * @return list<string>
 */
function balancedArguments(string $code, int $openParenthesis): array {
    $depth = 0;
    $topLevel = '';
    $length = strlen($code);
    $quote = null;

    for ($i = $openParenthesis; $i < $length; $i++) {
        $character = $code[$i];
        if ($quote !== null) {
            if ($character === '\\') {
                $i++;
            } elseif ($character === $quote) {
                $quote = null;
            }

            continue;
        }
        if ($character === "'" || $character === '"') {
            $quote = $character;

            continue;
        }
        if ($character === '(' || $character === '[') {
            $depth++;

            continue;
        }
        if ($character === ')' || $character === ']') {
            $depth--;
            if ($depth === 0) {
                break;
            }

            continue;
        }
        if ($depth === 1) {
            $topLevel .= $character;
        }
    }

    preg_match_all('/(?:^|,)\s*(?:\/\/[^\n]*\n\s*)*([a-zA-Z_]\w*)\s*:(?!:)/', $topLevel, $matches);

    return $matches[1];
}

/**
 * @param list<string> $named
 * @param list<string> $errors
 */
function checkNamedArguments(ReflectionMethod $method, array $named, int $block, array &$errors): void {
    $known = array_map(static fn (ReflectionParameter $parameter): string => $parameter->getName(), $method->getParameters());
    foreach ($named as $name) {
        if (!in_array($name, $known, true)) {
            $errors[] = sprintf('block %d: %s::%s() has no parameter $%s', $block, $method->getDeclaringClass()->getName(), $method->getName(), $name);
        }
    }
}

preg_match_all('/```php\n(.*?)```/s', $readme, $blocks);

foreach ($blocks[1] as $number => $code) {
    $block = $number + 1;
    $source = str_starts_with(ltrim($code), '<?php') ? $code : "<?php\n" . $code;
    $isForeignExample = str_contains($code, FOREIGN_NAMESPACE);

    try {
        $tokens = token_get_all($source, TOKEN_PARSE);
        unset($tokens);
    } catch (ParseError $e) { // @phpstan-ignore catch.neverThrown
        $errors[] = sprintf('block %d does not parse: %s', $block, $e->getMessage());

        continue;
    }

    $imports = [];
    preg_match_all('/^use\s+([\w\\\\]+)(?:\s+as\s+(\w+))?\s*;/m', $code, $uses, PREG_SET_ORDER);
    foreach ($uses as $use) {
        $class = $use[1];
        $imports[$use[2] ?? substr((string) strrchr('\\' . $class, '\\'), 1)] = $class;
        if (str_starts_with($class, 'Cassandra\\') && !class_exists($class) && !interface_exists($class) && !enum_exists($class)) {
            $errors[] = sprintf('block %d imports unknown %s', $block, $class);
        }
    }
    preg_match_all('/^use\s+([\w\\\\]+)\\\\\{([^}]+)\}\s*;/ms', $code, $groupUses, PREG_SET_ORDER);
    foreach ($groupUses as $groupUse) {
        foreach (array_filter(array_map('trim', explode(',', $groupUse[2]))) as $short) {
            $class = $groupUse[1] . '\\' . $short;
            $imports[$short] = $class;
            if (!class_exists($class)) {
                $errors[] = sprintf('block %d imports unknown %s', $block, $class);
            }
        }
    }

    $resolve = static function (string $name) use ($imports): ?string {
        $name = ltrim($name, '\\');
        if (isset($imports[$name])) {
            return $imports[$name];
        }

        return str_starts_with($name, 'Cassandra\\') ? $name : null;
    };

    if (!$isForeignExample) {
        preg_match_all('/\bnew\s+([\\\\\w]+)\s*\(/', $code, $instantiations, PREG_OFFSET_CAPTURE);
        foreach ($instantiations[1] as $index => [$name]) {
            $class = $resolve($name);
            if ($class === null || in_array($class, FOREIGN_CLASSES, true)) {
                continue;
            }
            if (!class_exists($class)) {
                $errors[] = sprintf('block %d instantiates unknown %s', $block, $class);

                continue;
            }
            $constructor = (new ReflectionClass($class))->getConstructor();
            if ($constructor !== null) {
                $open = $instantiations[0][$index][1] + strlen($instantiations[0][$index][0]) - 1;
                checkNamedArguments($constructor, balancedArguments($code, $open), $block, $errors);
            }
        }

        preg_match_all('/([\\\\\w]+)::(\$?\w+)(\s*\()?/', $code, $statics, PREG_SET_ORDER | PREG_OFFSET_CAPTURE);
        foreach ($statics as $static) {
            $class = $resolve($static[1][0]);
            if ($class === null || !class_exists($class) && !enum_exists($class)) {
                continue;
            }
            $member = $static[2][0];
            if ($member === 'class') {
                continue;
            }
            if (isset($static[3])) {
                if (!method_exists($class, $member)) {
                    $errors[] = sprintf('block %d calls unknown %s::%s()', $block, $class, $member);

                    continue;
                }
                $open = $static[3][1] + strlen($static[3][0]) - 1;
                checkNamedArguments(new ReflectionMethod($class, $member), balancedArguments($code, $open), $block, $errors);
            } elseif (!defined($class . '::' . $member)) {
                $errors[] = sprintf('block %d uses unknown %s::%s', $block, $class, $member);
            }
        }

        // $conn is always a Connection in the README; $statement and $s1-style handles a Statement.
        foreach ([Connection::class => '\$conn', Statement::class => '\$(?:statement|stmt|ready|fast|slow|s\d)'] as $class => $variable) {
            preg_match_all('/' . $variable . '->(\w+)\s*\(/', $code, $calls, PREG_SET_ORDER | PREG_OFFSET_CAPTURE);
            foreach ($calls as $call) {
                $method = $call[1][0];
                if (!method_exists($class, $method)) {
                    $errors[] = sprintf('block %d calls unknown %s::%s()', $block, $class, $method);

                    continue;
                }
                $open = $call[0][1] + strlen($call[0][0]) - 1;
                checkNamedArguments(new ReflectionMethod($class, $method), balancedArguments($code, $open), $block, $errors);
            }
        }
    }

    // A block that talks to no server and borrows no variable can simply be run.
    $body = preg_replace('/^\s*<\?php\s*/', '', $code);
    if (!$isForeignExample && preg_match('/\$(?!this\b)\w+/', (string) preg_replace('/\$\w+\s*=[^=]/', '', (string) $body)) === 0
        && !str_contains($code, 'require ') && !str_contains($code, 'new Connection')) {
        try {
            (static function (string $code): void {
                eval(preg_replace('/^\s*<\?php\s*/', '', $code));
            })($code);
        } catch (Throwable $e) {
            $errors[] = sprintf('block %d fails when run: %s: %s', $block, get_class($e), $e->getMessage());
        }
    }
}

// Internal links, by GitHub's slug rules.
$withoutCode = (string) preg_replace('/```.*?```/s', '', $readme);
preg_match_all('/^#{1,6} +(.+)$/m', $withoutCode, $headings);
preg_match_all('/^(.+)\n(?:-{3,}|={3,})$/m', $withoutCode, $underlined);
$anchors = [];
foreach (array_merge($headings[1], $underlined[1]) as $heading) {
    $anchor = strtolower(trim($heading));
    $anchor = (string) preg_replace('/[^\p{L}\p{N} _-]/u', '', $anchor);
    $anchors[str_replace(' ', '-', $anchor)] = true;
}
preg_match_all('/\]\(#([^)]+)\)/', $withoutCode, $links);
foreach ($links[1] as $link) {
    if (!isset($anchors[$link]) && !isset($anchors[(string) preg_replace('/-\d+$/', '', $link)])) {
        $errors[] = sprintf('link #%s has no heading', $link);
    }
}

// Classes named in prose.
preg_match_all('/`(\\\\?Cassandra\\\\[\w\\\\]+)/', $withoutCode, $prose);
foreach (array_unique($prose[1]) as $class) {
    $class = ltrim($class, '\\');
    if (!class_exists($class) && !interface_exists($class) && !enum_exists($class) && !in_array($class, FOREIGN_CLASSES, true) && !is_dir(dirname(__DIR__) . '/src/' . str_replace('\\', '/', substr($class, strlen('Cassandra\\'))))) {
        $errors[] = sprintf('prose names unknown %s', $class);
    }
}

printf("%d PHP blocks: %d problems\n", count($blocks[1]), count($errors));
foreach ($errors as $error) {
    echo "  {$error}\n";
}

exit($errors === [] ? 0 : 1);
