<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Value\EncodeOption\DateEncodeOption;
use Cassandra\Value\EncodeOption\DurationEncodeOption;
use Cassandra\Value\EncodeOption\TimeEncodeOption;
use Cassandra\Value\EncodeOption\TimestampEncodeOption;
use Cassandra\Value\EncodeOption\VarintEncodeOption;

final class ValueEncodeConfig {
    private static ?self $default = null;

    public function __construct(
        public readonly DateEncodeOption $dateEncodeOption = DateEncodeOption::AS_STRING,
        public readonly DurationEncodeOption $durationEncodeOption = DurationEncodeOption::AS_STRING,
        public readonly TimeEncodeOption $timeEncodeOption = TimeEncodeOption::AS_STRING,
        public readonly TimestampEncodeOption $timestampEncodeOption = TimestampEncodeOption::AS_STRING,
        public readonly VarintEncodeOption $varintEncodeOption = VarintEncodeOption::AS_STRING,
    ) {
    }

    public static function default(): self {

        self::$default ??= new self();

        return self::$default;
    }
}
