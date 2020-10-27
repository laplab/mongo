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

#include "mongo/db/exec/sbe/ctp/expression.h"

#include "mongo/util/assert_util.h"

namespace mongo::sbe::ctp {

std::unique_ptr<EExpression> Expression::build(const ExpressionPool& exprs, BuildContext& context) const {
    switch (type) {
        case ExpressionType::Placeholder:
            return std::move(context.indexedPlaceholders[placeholderIndex]);

        case ExpressionType::FunctionCall: {
            if (stringValue == "fail") {
                invariant(childrenCount == 2);
                auto code = static_cast<ErrorCodes::Error>(exprs.get(children[0]).int32Value);
                std::string message{exprs.get(children[1]).stringValue};
                return makeE<EFail>(code, message);
            }

            auto compiledChildren = makeEs();
            compiledChildren.reserve(childrenCount);

            for (uint64_t i = 0; i < childrenCount; i++) {
                auto compiledChild = exprs.get(children[i]).build(exprs, context);
                compiledChildren.emplace_back(std::move(compiledChild));
            }

            if (stringValue == "nullOrMissing") {
                invariant(childrenCount == 1);
                auto argument = std::move(compiledChildren[0]);
                auto variable = static_cast<EVariable*>(argument.get());
                return makeE<EPrimBinary>(
                    EPrimBinary::logicOr,
                    makeE<EPrimUnary>(EPrimUnary::logicNot,
                                            makeE<EFunction>("exists", makeEs(variable->clone()))),
                    makeE<EFunction>("isNull", makeEs(variable->clone()))
                );
            }

            if (stringValue == "toInt32") {
                invariant(childrenCount == 1);
                return makeE<ENumericConvert>(std::move(compiledChildren[0]), value::TypeTags::NumberInt32);
            }

            return makeE<EFunction>(stringValue, std::move(compiledChildren));
        }

        case ExpressionType::Nothing:
            return makeE<EConstant>(value::TypeTags::Nothing, value::bitcastFrom<int64_t>(0));

        case ExpressionType::Null:
            return makeE<EConstant>(value::TypeTags::Null, value::bitcastFrom<int64_t>(0));

        case ExpressionType::Int32:
            return makeE<EConstant>(value::TypeTags::NumberInt32, value::bitcastFrom<int32_t>(int32Value));

        case ExpressionType::Int64:
            return makeE<EConstant>(value::TypeTags::NumberInt64, value::bitcastFrom<int64_t>(int64Value));

        case ExpressionType::Boolean:
            return makeE<EConstant>(value::TypeTags::Boolean, value::bitcastFrom<bool>(boolValue));

        case ExpressionType::Or:
        case ExpressionType::And: {
            invariant(childrenCount == 2);

            auto left = exprs.get(children[0]).build(exprs, context);
            auto right = exprs.get(children[1]).build(exprs, context);

            auto opType = type == ExpressionType::Or ? EPrimBinary::logicOr : EPrimBinary::logicAnd;
            return makeE<EPrimBinary>(opType, std::move(left), std::move(right));
        }

        case ExpressionType::If: {
            invariant(childrenCount == 3);

            auto ifExpr = exprs.get(children[0]).build(exprs, context);
            auto thenExpr = exprs.get(children[1]).build(exprs, context);
            auto elseExpr = exprs.get(children[2]).build(exprs, context);

            return makeE<EIf>(std::move(ifExpr), std::move(thenExpr), std::move(elseExpr));
        }

        case ExpressionType::VariableAssignment: {
            if (childrenCount > 1) {
                exprs.get(children[1]).build(exprs, context);
            }

            auto variableValue = exprs.get(children[0]).build(exprs, context);
            auto& frame = context.variableStack.back();
            frame.binds.emplace_back(std::move(variableValue));

            return nullptr;
        }

        case ExpressionType::Variable: {
            auto& frame = context.variableStack[frameIndex];
            return makeE<EVariable>(frame.frameId, variableId);
        }

        case ExpressionType::Let: {
            auto frameId = context.frameIdGenerator.generate();
            context.variableStack.push_back({frameId, {}});

            // Fill binds vector located on the top of 'variableStack'.
            exprs.get(children[0]).build(exprs, context);

            // Build 'in' expression.
            auto inBodyExpr = exprs.get(children[1]).build(exprs, context);

            auto& frame = context.variableStack.back();
            auto letExpr = makeE<ELocalBind>(frame.frameId, std::move(frame.binds), std::move(inBodyExpr));
            context.variableStack.pop_back();

            return letExpr;
        }

        case ExpressionType::String: {
            auto [tag, value] = value::makeNewString(stringValue);
            return makeE<EConstant>(tag, value);
        }

        case ExpressionType::Not: {
            invariant(childrenCount == 1);
            auto argument = exprs.get(children[0]).build(exprs, context);
            return makeE<EPrimUnary>(EPrimUnary::logicNot, std::move(argument));
        }

        case ExpressionType::Skunk: {
            auto [tag, value] = value::makeNewString(SKUNK_STRING);
            return makeE<EConstant>(tag, value);
        }

        case ExpressionType::None:
            MONGO_UNREACHABLE;
    }

    return nullptr;
}

}