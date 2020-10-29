/**
 *    Copyright (C) 2020-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include <cmath>
#include <limits>

#include "mongo/db/exec/sbe/ctp/parser.h"
#include "mongo/db/exec/sbe/expression_test_base.h"
#include "mongo/db/exec/sbe/util/debug_print.h"

namespace mongo::sbe {

using namespace ctp;

class SBECompileTimeParserTest : public EExpressionTestFixture {
protected:
    SBECompileTimeParserTest() = default;

    void assertExpr(std::unique_ptr<EExpression> expr, const std::string& expectedOutput) {
        const auto actualOutput = printer.print(expr.get());
        ASSERT_EQ(actualOutput, expectedOutput);
    }

    DebugPrinter printer;
};

TEST_F(SBECompileTimeParserTest, TestBasic) {
    constexpr auto code1 = "Nothing"_sbe;
    assertExpr(code1(_frameIdGenerator), "Nothing ");

    constexpr auto code2 = "Null"_sbe;
    assertExpr(code2(_frameIdGenerator), "null ");

    constexpr auto code3 = "123"_sbe;
    assertExpr(code3(_frameIdGenerator), "123 ");

    constexpr auto code4 = "123l"_sbe;
    assertExpr(code4(_frameIdGenerator), "123l ");

    constexpr auto code5 = "true"_sbe;
    assertExpr(code5(_frameIdGenerator), "true ");

    constexpr auto code6 = "false"_sbe;
    assertExpr(code6(_frameIdGenerator), "false ");

    constexpr auto code7 = "'string with spaces'"_sbe;
    assertExpr(code7(_frameIdGenerator), "\"string with spaces\" ");

    constexpr auto code8 = "\"double quoted string with spaces\""_sbe;
    assertExpr(code8(_frameIdGenerator), "\"double quoted string with spaces\" ");

}

TEST_F(SBECompileTimeParserTest, TestFunction) {
    constexpr auto code = "getElement(Nothing, Nothing)"_sbe;
    auto expr = code(_frameIdGenerator);

    assertExpr(std::move(expr), "getElement (Nothing, Nothing) ");
}

TEST_F(SBECompileTimeParserTest, TestBuiltins) {
    constexpr auto code1 = "fail(123, 'Failed to do something')"_sbe;
    assertExpr(code1(_frameIdGenerator), "fail ( 123 ,Failed to do something) ");

    constexpr auto code2 = "toInt32(123l)"_sbe;
    assertExpr(code2(_frameIdGenerator), "convert ( 123l, int32) ");

    constexpr auto code3 = R"(
        let x = 123, y = 456 in {
            nullOrMissing(x) || nullOrMissing(y)
        }
    )"_sbe;
    assertExpr(code3(_frameIdGenerator), "let [l1.0 = 123, l1.1 = 456] ( ( ! exists (l1.0) || isNull (l1.0) ) || ( ! exists (l1.1) || isNull (l1.1) ) ) ");
}

TEST_F(SBECompileTimeParserTest, TestLogicOperators) {
    constexpr auto code1 = "true || false"_sbe;
    assertExpr(code1(_frameIdGenerator), "( true || false ) ");

    constexpr auto code2 = "false || true"_sbe;
    assertExpr(code2(_frameIdGenerator), "( false || true ) ");

    constexpr auto code3 = "true && false"_sbe;
    assertExpr(code3(_frameIdGenerator), "( true && false ) ");

    constexpr auto code4 = "false && true"_sbe;
    assertExpr(code4(_frameIdGenerator), "( false && true ) ");

    constexpr auto code5 = "false && true || false"_sbe;
    assertExpr(code5(_frameIdGenerator), "( ( false && true ) || false ) ");

    constexpr auto code6 = "true || false && true"_sbe;
    assertExpr(code6(_frameIdGenerator), "( true || ( false && true ) ) ");

    constexpr auto code7 = "true || false || true"_sbe;
    assertExpr(code7(_frameIdGenerator), "( ( true || false ) || true ) ");

    constexpr auto code8 = "true && false && true"_sbe;
    assertExpr(code8(_frameIdGenerator), "( ( true && false ) && true ) ");

    constexpr auto code9 = "!true"_sbe;
    assertExpr(code9(_frameIdGenerator), "! true ");

    constexpr auto code10 = "! (true || false)"_sbe;
    assertExpr(code10(_frameIdGenerator), "! ( true || false ) ");

    constexpr auto code11 = "false && !true"_sbe;
    assertExpr(code11(_frameIdGenerator), "( false && ! true ) ");

    constexpr auto code12 = "!false && true"_sbe;
    assertExpr(code12(_frameIdGenerator), "( ! false && true ) ");
}

TEST_F(SBECompileTimeParserTest, TestArithmeticOperators) {
    constexpr auto code1 = "1 + 2"_sbe;
    assertExpr(code1(_frameIdGenerator), "( 1 + 2 ) ");

    constexpr auto code2 = "1 - 2"_sbe;
    assertExpr(code2(_frameIdGenerator), "( 1 - 2 ) ");

    constexpr auto code3 = "1 - 2 + 3"_sbe;
    assertExpr(code3(_frameIdGenerator), "( ( 1 - 2 ) + 3 ) ");

    constexpr auto code4 = "1 - (2 + 3)"_sbe;
    assertExpr(code4(_frameIdGenerator), "( 1 - ( 2 + 3 ) ) ");
}

TEST_F(SBECompileTimeParserTest, TestOperatorPrecedence) {
    constexpr auto code1 = "1 + 2 && 3 - 4"_sbe;
    assertExpr(code1(_frameIdGenerator), "( ( 1 + 2 ) && ( 3 - 4 ) ) ");

    constexpr auto code2 = "1 + 2 || 3 - 4"_sbe;
    assertExpr(code2(_frameIdGenerator), "( ( 1 + 2 ) || ( 3 - 4 ) ) ");
}

TEST_F(SBECompileTimeParserTest, TestParens) {
    constexpr auto code1 = "(true)"_sbe;
    assertExpr(code1(_frameIdGenerator), "true ");

    constexpr auto code2 = "(((true)))"_sbe;
    assertExpr(code2(_frameIdGenerator), "true ");

    constexpr auto code3 = "((true)) || (false)"_sbe;
    assertExpr(code3(_frameIdGenerator), "( true || false ) ");

    constexpr auto code5 = "(true || false) && true"_sbe;
    assertExpr(code5(_frameIdGenerator), "( ( true || false ) && true ) ");

    constexpr auto code6 = "true && (true || false)"_sbe;
    assertExpr(code6(_frameIdGenerator), "( true && ( true || false ) ) ");
}

TEST_F(SBECompileTimeParserTest, TestPlaceholders) {
    constexpr auto code = "getElement({0}, {1})"_sbe;
    auto expr = code(
        _frameIdGenerator,
        makeE<EConstant>(value::TypeTags::NumberInt64, value::bitcastFrom<int64_t>(123)),
        makeE<EConstant>(value::TypeTags::Nothing, value::bitcastFrom<int64_t>(0))
    );

    assertExpr(std::move(expr), "getElement (123l, Nothing) ");
}

TEST_F(SBECompileTimeParserTest, TestIf) {
    constexpr auto code1 = "if true { 123 } else { 456 }"_sbe;
    assertExpr(code1(_frameIdGenerator), "if (true, 123, 456) ");

    constexpr auto code2 = "if true || false && true { 123 } else { 456 }"_sbe;
    assertExpr(code2(_frameIdGenerator), "if (( true || ( false && true ) ), 123, 456) ");

    constexpr auto code3 = R"(
        if true {
            if false {
                123
            } else {
                456
            }
        } else {
            if Nothing {
                789
            } else {
                987
            }
        }
    )"_sbe;
    assertExpr(code3(_frameIdGenerator), "if (true, if (false, 123, 456), if (Nothing, 789, 987)) ");

    constexpr auto code4 = R"(
        if true {
            123
        } elif false {
            456
        } elif Nothing {
            789
        } else {
            0
        }
    )"_sbe;
    assertExpr(code4(_frameIdGenerator), "if (true, 123, if (false, 456, if (Nothing, 789, 0))) ");
}

TEST_F(SBECompileTimeParserTest, TestLet) {
    constexpr auto code1 = R"(
        let x = true, y = false in {
            x || y
        }
    )"_sbe;
    assertExpr(code1(_frameIdGenerator), "let [l1.0 = true, l1.1 = false] ( l1.0 || l1.1 ) ");

    constexpr auto code2 = R"(
        let a = true, b = false in {
            let c = 1, d = Nothing in {
                if a {
                    d && b
                } else {
                    c || d
                }
            }
        }
    )"_sbe;
    assertExpr(code2(_frameIdGenerator), "let [l2.0 = true, l2.1 = false] let [l3.0 = 1, l3.1 = Nothing] if (l2.0, ( l3.1 && l2.1 ), ( l3.0 || l3.1 )) ");
}

TEST_F(SBECompileTimeParserTest, TestSkunk) {
    constexpr auto code = u8"🦨"_sbe;
    assertExpr(code(_frameIdGenerator), str::stream() << '"' << SKUNK_STRING << "\" ");
}

}  // namespace mongo::sbe
