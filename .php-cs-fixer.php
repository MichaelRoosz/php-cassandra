<?php

declare(strict_types=1);

$finder = PhpCsFixer\Finder::create()
    ->in(__DIR__)
    ->name('*.php')
    ->notPath('/^vendor\//')
;

$config = new PhpCsFixer\Config();

return $config->setRules([
    '@PSR2' => false,
    'declare_strict_types' => true,
    'array_indentation' => true,
    'array_syntax' => [
        'syntax' => 'short',                             // short array() => [] , long [] => array()
    ],
    'blank_line_after_namespace' => true,
    'blank_line_after_opening_tag' => true,
    'blank_line_before_statement' => true,              // blank before return, try, break, continue
    'single_space_around_construct' => true,
    'control_structure_braces' => true,
    'control_structure_continuation_position' => true,
    'declare_parentheses' => true,
    'no_multiple_statements_per_line' => true,
    'braces_position' => [
        'functions_opening_brace' => 'same_line',
        'classes_opening_brace' => 'same_line',
    ],
    'statement_indentation' => true,
    'no_extra_blank_lines' => true,
    'cast_spaces' => true,
    'class_definition' => [
        'single_line' => false,
    ],
    'clean_namespace' => true,
    'combine_consecutive_issets' => false,
    'combine_consecutive_unsets' => false,                // unset($a); unset($b); => unset($a, $b);
    'concat_space' => ['spacing' => 'one'],        // $foo = 'bar' . 3 . 'baz' . 'qux';
    'constant_case' => true,                             // nuLL, FALSE, True => null, false, true
    'elseif' => true,
    'encoding' => true,
    'type_declaration_spaces' => ['elements' => ['function', 'property']],
    'include' => true,                                  // include("sample.php"); => include_once "sample.php"
    'indentation_type' => true,
    'linebreak_after_opening_tag' => true,
    'list_syntax' =>  ['syntax' => 'short'],
    'lowercase_cast' => true,       // (InTegEr),(BOOLEAN), => (integer),(boolean)
    'lowercase_keywords' => true,    // WHILE, FOREACH => while, foreach
    'lowercase_static_reference' => true,
    'magic_constant_casing' => true,
    'magic_method_casing' => true,
    'native_function_casing' => true,
    'native_type_declaration_casing' => true,
    'no_blank_lines_after_class_opening' => true,
    'no_empty_statement' => true,
    'no_leading_import_slash' => true,
    'no_mixed_echo_print' => ['use' => 'echo'],
    'no_spaces_after_function_name' => true,
    'no_spaces_around_offset' => true,
    'spaces_inside_parentheses' => ['space' => 'none'],
    'no_trailing_whitespace' => true,
    'no_whitespace_in_blank_line' => true,
    'operator_linebreak' => ['only_booleans' => true],
    'ordered_class_elements' =>  ['sort_algorithm' => 'alpha'],
    'short_scalar_cast' => true,
    'single_blank_line_at_eof' => true,
    'single_class_element_per_statement' => ['elements' => ['const', 'property']],
    'single_quote' => true,
    'ternary_operator_spaces' => true,
    'trailing_comma_in_multiline' => true,
    'whitespace_after_comma_in_array' => true,
    'visibility_required' => true,
    'return_type_declaration'=> ['space_before' => 'none'],
])
    ->setIndent('    ')
    ->setLineEnding("\n")
    ->setFinder($finder)
    ->setRiskyAllowed(true)
;
