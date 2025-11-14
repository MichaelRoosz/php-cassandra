<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

enum ProtocolVersion: int {
    case V3 = 3;
    case V4 = 4;
    case V5 = 5;

    const OPTION_FORMAT_V3 = '3/v3';
    const OPTION_FORMAT_V4 = '4/v4';
    const OPTION_FORMAT_V5 = '5/v5';

    const PREFRED_ORDER = [
        self::V5,
        self::V4,
        self::V3,
    ];

    const CASES_IN_OPTION_FORMAT = [
        self::OPTION_FORMAT_V3,
        self::OPTION_FORMAT_V4,
        self::OPTION_FORMAT_V5,
    ];

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

    public static function fromOptionFormat(string $versionInOptionFormat): ?self
    {
        return match ($versionInOptionFormat) {
            self::OPTION_FORMAT_V3 => self::V3,
            self::OPTION_FORMAT_V4 => self::V4,
            self::OPTION_FORMAT_V5 => self::V5,
            default => null,
        };
    }

    public function asIncomingVersion(): int {
        return $this->value + 0x80;
    }

    public function supports(ProtocolVersion $other): bool {
        return $this->value >= $other->value;
    }

    public function toOptionFormat(): string
    {
        return match ($this) {
            self::V3 => self::OPTION_FORMAT_V3,
            self::V4 => self::OPTION_FORMAT_V4,
            self::V5 => self::OPTION_FORMAT_V5,
        };
    }
}
