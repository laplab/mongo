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

#include "mongo/db/exec/sbe/expression_test_base.h"
#include "mongo/db/exec/sbe/ctp/sbe_compile_time_parser.h"
#include "mongo/db/exec/sbe/util/debug_print.h"

namespace mongo::sbe {

using namespace ctp;

class SBECompileTimeParserTest : public EExpressionTestFixture {
protected:
    SBECompileTimeParserTest() = default;

    void assertExpr(std::unique_ptr<EExpression>& expr, const std::string& expectedOutput) {
        const auto actualOutput = printer.print(expr.get());
        ASSERT_EQ(actualOutput, expectedOutput);
    }

    DebugPrinter printer;
};

TEST_F(SBECompileTimeParserTest, TestFunction) {
    constexpr auto code = "getElement({0}, {1})"_sbe;
    auto expr = code.build(makeE<EConstant>(value::TypeTags::Nothing, 0), makeE<EConstant>(value::TypeTags::Nothing, 0));

    assertExpr(expr, "getElement (Nothing, Nothing) ");
}

}  // namespace mongo::sbe
