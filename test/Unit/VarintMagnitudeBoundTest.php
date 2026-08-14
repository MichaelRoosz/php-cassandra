<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Value\Decimal;
use Cassandra\Value\Varint;

/**
 * A varint's magnitude is bounded, because converting one is quadratic.
 *
 * {@see \Cassandra\StringMath\DecimalCalculator\Native} — the fallback a build
 * without gmp and without bcmath uses — walks the whole number once per byte, so
 * an unbounded magnitude is a decode-side denial of service rather than merely a
 * slow value: the cell length is the peer's to choose and is bounded only by the
 * frame, and a few kilobytes of it cost minutes of CPU.
 *
 * The bound is applied to the bytes on the way in and to the digits on the way
 * out, so the two sides agree: nothing is written that could not be read back.
 */
final class VarintMagnitudeBoundTest extends AbstractUnitTestCase {
    public function testABinaryAtTheByteBoundStillAnswersToTheDigitBound(): void {
        // The byte bound is a pre-filter, not the decision. 1701 bytes of 0x7f
        // is 13607 significant bits, which spells 4097 digits — one past the
        // digit bound, and so refused by that once the (now cost-bounded)
        // conversion has run, rather than by the byte check it just got past.
        $widest = str_repeat("\x7f", Varint::MAX_MAGNITUDE_BYTES);

        try {
            Varint::fromBinary($widest);
            $this->fail('expected the digit bound to refuse a value the byte bound admitted');
        } catch (ValueException $e) {
            $this->assertSame(ExceptionCode::VALUE_VARINT_MAGNITUDE_TOO_LARGE->value, $e->getCode());
            $this->assertSame(
                ['digits' => 4097, 'max_digits' => Varint::MAX_MAGNITUDE_DIGITS],
                $e->getContext(),
                'reported by the digit check, which is the authority',
            );
        }
    }

    public function testADecimalWhoseUnscaledPartIsTooWideIsRefused(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_DECIMAL_UNSCALED_TOO_LARGE->value);

        // No scale at all, and an unscaled part past the bound — the case the
        // scale bound has nothing to say about.
        new Decimal('1e' . Varint::MAX_MAGNITUDE_DIGITS);
    }

    public function testALargeScaleWithATinyUnscaledPartIsStillAccepted(): void {
        // The two bounds are orthogonal. This value is enormous to spell and
        // trivial to convert: the unscaled part is 1, and its leading zeros are
        // dropped before anything is converted.
        $value = new Decimal('1e-100000');

        $this->assertSame('0.' . str_repeat('0', 99_999) . '1', $value->getValue());
        $this->assertSame(1, strlen(ltrim(str_replace(['-', '.'], '', $value->getValue()), '0')));
    }

    public function testANegativeScaleAtTheDigitBoundIsAccepted(): void {
        // A negative scale multiplies the value up, so its zeros are significant
        // digits of the unscaled varint: the widest such value is one whose
        // digits together reach the digit bound exactly.
        $atLimit = Decimal::fromBinary(pack('N', -(Varint::MAX_MAGNITUDE_DIGITS - 1)) . chr(1));

        $this->assertNotNull($atLimit);
        $this->assertSame('1' . str_repeat('0', Varint::MAX_MAGNITUDE_DIGITS - 1), $atLimit->getValue());

        // And it survives the trip back out, which is why the decode side is
        // held to the encode side's bound.
        $this->assertSame($atLimit->getValue(), Decimal::fromBinary($atLimit->getBinary())?->getValue());
    }

    public function testANegativeScaleNeverWidensZero(): void {
        // Zero times any power of ten is zero, so even the most extreme scale
        // the field can carry stays readable.
        $zero = Decimal::fromBinary(pack('N', -100_000) . chr(0));

        $this->assertNotNull($zero);
        $this->assertSame('0', $zero->getValue());
    }

    public function testANegativeScalePastTheDigitBoundIsReportedAsAScaleFailure(): void {
        // The regression this exists for: a negative scale hits the varint digit
        // bound long before MAX_SCALE_MAGNITUDE, and the expansion used to run
        // first — so the failure surfaced as a digit count the peer never sent
        // rather than against the scale it declared.
        foreach ([-Varint::MAX_MAGNITUDE_DIGITS, -5_000, -100_000] as $scale) {
            try {
                Decimal::fromBinary(pack('N', $scale) . chr(1));
                $this->fail('expected a scale of ' . $scale . ' to be refused');
            } catch (ValueException $e) {
                $this->assertSame(ExceptionCode::VALUE_DECIMAL_SCALE_OUT_OF_RANGE->value, $e->getCode(), (string) $scale);
                $this->assertSame($scale, $e->getContext()['scale'] ?? null, (string) $scale);
            }
        }
    }

    public function testAVarintAtTheDigitBoundIsAccepted(): void {
        $atLimit = str_repeat('9', Varint::MAX_MAGNITUDE_DIGITS);

        $value = new Varint($atLimit);

        $this->assertSame($atLimit, $value->asString());
        $this->assertSame($atLimit, Varint::fromBinary($value->getBinary())?->asString());
    }

    public function testAVarintPastTheDigitBoundIsRefused(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_VARINT_MAGNITUDE_TOO_LARGE->value);

        new Varint(str_repeat('9', Varint::MAX_MAGNITUDE_DIGITS + 1));
    }

    public function testLeadingZerosDoNotCountTowardsTheDigitBound(): void {
        // They are a spelling rather than a value, and are dropped before
        // anything is converted.
        $value = new Varint(str_repeat('0', 10_000) . '7');

        $this->assertSame('7', $value->asString());
    }

    public function testTheNegativeSignDoesNotCountTowardsTheDigitBound(): void {
        $atLimit = '-' . str_repeat('9', Varint::MAX_MAGNITUDE_DIGITS);

        $this->assertSame($atLimit, (new Varint($atLimit))->asString());
    }

    public function testTheTwoBoundsMeet(): void {
        // Anything getBinary() can produce has to get past the byte bound, or
        // this class would write values to a node it could never read back.
        $atLimit = new Varint(str_repeat('9', Varint::MAX_MAGNITUDE_DIGITS));

        $this->assertLessThanOrEqual(Varint::MAX_MAGNITUDE_BYTES, strlen($atLimit->getBinary()));
        $this->assertLessThanOrEqual(
            Varint::MAX_MAGNITUDE_BYTES,
            strlen((new Varint('-' . str_repeat('9', Varint::MAX_MAGNITUDE_DIGITS)))->getBinary()),
        );
    }

    public function testWideBinaryIsRefusedBeforeItIsConverted(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_VARINT_MAGNITUDE_TOO_LARGE->value);

        Varint::fromBinary(str_repeat("\x7f", Varint::MAX_MAGNITUDE_BYTES + 1));
    }
}
