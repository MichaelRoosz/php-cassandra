<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

enum ProtocolVersion: int {
    case V3 = 3;
    case V4 = 4;
    case V5 = 5;

    public const CASES_IN_OPTION_FORMAT = [
        self::OPTION_FORMAT_V3,
        self::OPTION_FORMAT_V4,
        self::OPTION_FORMAT_V5,
    ];

    public const OPTION_FORMAT_V3 = '3/v3';
    public const OPTION_FORMAT_V4 = '4/v4';
    public const OPTION_FORMAT_V5 = '5/v5';

    public const PREFRED_ORDER = [
        self::V5,
        self::V4,
        self::V3,
    ];

    public function asIncomingVersion(): int {
        return $this->value + 0x80;
    }

    public static function fromOptionFormat(string $versionInOptionFormat): ?self {
        return match ($versionInOptionFormat) {
            self::OPTION_FORMAT_V3 => self::V3,
            self::OPTION_FORMAT_V4 => self::V4,
            self::OPTION_FORMAT_V5 => self::V5,
            default => null,
        };
    }

    /**
     * @param ProtocolVersion[] $availableVersions
     * @param ProtocolVersion[] $allowedVersions
     */
    public static function getHighestSupportedVersion(array $availableVersions, array $allowedVersions): ?ProtocolVersion {

        $possibleVersions = [];
        foreach ($allowedVersions as $allowedVersion) {
            if (in_array($allowedVersion, $availableVersions, true)) {
                $possibleVersions[] = $allowedVersion;
            }
        }

        if (!$possibleVersions) {
            return null;
        }

        foreach (self::PREFRED_ORDER as $version) {
            if (in_array($version, $possibleVersions, true)) {
                return $version;
            }
        }

        return null;
    }

    public function supports(ProtocolVersion $other): bool {
        return $this->value >= $other->value;
    }

    public function inOptionFormat(): string {
        return match ($this) {
            self::V3 => self::OPTION_FORMAT_V3,
            self::V4 => self::OPTION_FORMAT_V4,
            self::V5 => self::OPTION_FORMAT_V5,
        };
    }
}
