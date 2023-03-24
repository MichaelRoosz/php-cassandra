<?php

declare(strict_types=1);

namespace Cassandra\Type;

trait CommonToString {
    /**
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Response\Exception
     */
    public function __toString(): string {
        /**
         *  @throws \Cassandra\Type\Exception
         *  @throws \Cassandra\Response\Exception
         * */
        $json = json_encode(
            $this->parseValue(),
            JSON_PRETTY_PRINT
            | JSON_PRESERVE_ZERO_FRACTION
            | JSON_PARTIAL_OUTPUT_ON_ERROR
            | JSON_UNESCAPED_LINE_TERMINATORS
            | JSON_UNESCAPED_SLASHES
            | JSON_UNESCAPED_UNICODE
        );

        return $json === false ? '' : $json;
    }
}
