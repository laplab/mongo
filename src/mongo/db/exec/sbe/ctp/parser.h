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
#pragma once

#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mongo/db/exec/sbe/ctp/expression.h"
#include "mongo/db/exec/sbe/ctp/tokenizer.h"

namespace mongo::sbe::ctp {

constexpr ExpressionType getOperatorType(TokenType type) {
    switch (type) {
        case TokenType::And:
            return ExpressionType::And;
        case TokenType::Or:
            return ExpressionType::Or;
        case TokenType::Plus:
            return ExpressionType::Add;
        case TokenType::Minus:
            return ExpressionType::Subtract;
        default:
            throw std::logic_error("Expected keyword token type");
    }
}

constexpr ExpressionType getKeywordType(TokenType type) {
    switch (type) {
        case TokenType::Nothing:
            return ExpressionType::Nothing;
        case TokenType::Null:
            return ExpressionType::Null;
        case TokenType::Skunk:
            return ExpressionType::Skunk;
        default:
            throw std::logic_error("Expected keyword token type");
    }
}

using BindingPower = uint8_t;
constexpr std::pair<BindingPower, BindingPower> getBindingPower(TokenType type) {
    switch (type) {
        case TokenType::Or:
            return {1, 2};
        case TokenType::And:
            return {3, 4};
        case TokenType::Not:
            return {0, 5};
        case TokenType::Plus:
        case TokenType::Minus:
            return {5, 6};
        default:
            throw std::logic_error("Expected operator token type");
    }
}

class ExpressionIdStack {
public:
    constexpr ExpressionIdStack() : _currentPos(0), _stack() {}

    constexpr const ExpressionId* begin() const {
        return _stack;
    }

    constexpr const ExpressionId* end() const {
        return begin() + _currentPos;
    }

    constexpr void push(ExpressionId exprId) {
        if (_currentPos == MAX_SIZE) {
            throw std::logic_error("Maximum stack size exceeded");
        }
        _stack[_currentPos++] = exprId;
    }

    constexpr void pop() {
        if (_currentPos == 0) {
            throw std::logic_error("Stack is empty");
        }
        _currentPos--;
    }

    constexpr ExpressionId top() const {
        if (_currentPos == 0) {
            throw std::logic_error("Stack is empty");
        }
        return _stack[_currentPos - 1];
    }

private:
    static constexpr long long MAX_SIZE = 20;
    uint64_t _currentPos = 0;
    ExpressionId _stack[MAX_SIZE];
};

class Parser {
public:
    constexpr Parser(std::string_view input)
        : _tokenizer(input),
          _current(_tokenizer.next()),
          _pool(),
          _variableStack(),
          currentFrameIndex(0) {}

    constexpr ExpressionPool parse() {
        _pool.setRootId(parseInternal(0));
        consume(TokenType::Eof);
        return _pool;
    }

private:
    constexpr ExpressionId parseInternal(BindingPower minBp) {
        ExpressionId leftExprId = 0;
        switch (peek()) {
            case TokenType::LeftCurlyBrace:
                leftExprId = consumePlaceholder();
                break;
            case TokenType::Identifier:
                leftExprId = consumeVariableOrFunctionCall();
                break;
            case TokenType::Nothing:
            case TokenType::Null:
            case TokenType::Skunk:
                leftExprId = consumeKeyword();
                break;
            case TokenType::Integer:
                leftExprId = consumeInteger();
                break;
            case TokenType::Boolean:
                leftExprId = consumeBoolean();
                break;
            case TokenType::If:
                leftExprId = consumeIf();
                break;
            case TokenType::Let:
                leftExprId = consumeLet();
                break;
            case TokenType::String:
                leftExprId = consumeString();
                break;
            case TokenType::LeftParen:
                consume(TokenType::LeftParen);
                leftExprId = parseInternal(0);
                consume(TokenType::RightParen);
                break;
            case TokenType::Not: {
                consume(TokenType::Not);
                auto [_, rightBp] = getBindingPower(TokenType::Not);
                auto rightExpr = parseInternal(rightBp);
                auto [opExprId, opExpr] = _pool.allocate(ExpressionType::Not);
                opExpr.pushChild(rightExpr);
                leftExprId = opExprId;
                break;
            }
            default:
                throw std::logic_error("Unexpected token type");
        }

        while (true) {
            const auto type = peek();
            if (!isOperator(type)) {
                break;
            }

            auto [leftBp, rightBp] = getBindingPower(type);
            if (leftBp < minBp) {
                break;
            }

            advance();

            auto rightExprId = parseInternal(rightBp);

            auto [opExprId, opExpr] = _pool.allocate(getOperatorType(type));
            opExpr.pushChild(leftExprId);
            opExpr.pushChild(rightExprId);

            leftExprId = opExprId;
        }

        return leftExprId;
    }

    constexpr ExpressionId consumePlaceholder() {
        consume(TokenType::LeftCurlyBrace);
        if (_current.data.integer.is64Bit) {
            throw std::logic_error(
                "Only 32-bit integer literals are supported as placeholder indexes");
        }
        auto [exprId, expr] = _pool.allocate(static_cast<uint64_t>(_current.data.integer.value));
        consume(TokenType::Integer);
        consume(TokenType::RightCurlyBrace);
        return exprId;
    }

    constexpr ExpressionId consumeLet() {
        consume(TokenType::Let);

        uint64_t frameIndex = currentFrameIndex++;
        ExpressionId variableListId = 0;
        value::SlotId currentVariableId = 0;
        bool isFirst = true;
        while (true) {
            if (match(TokenType::Comma)) {
                consume(TokenType::Comma);
            } else if (!isFirst) {
                break;
            }

            auto variableName = _current.data.string;
            auto [existingSlotId, existingFrameIndex, isAlreadyDefined] =
                lookupVariableByName(variableName);
            if (isAlreadyDefined) {
                throw std::logic_error("Variable redifinition detected");
            }

            consume(TokenType::Identifier);
            consume(TokenType::Equals);

            auto variableValue = parseInternal(0);

            auto [variableExprId, variable] =
                _pool.allocate(variableName, currentVariableId++, frameIndex);
            variable.pushChild(variableValue);

            if (!isFirst) {
                variable.pushChild(variableListId);
            }
            variableListId = variableExprId;

            isFirst = false;
        }

        consume(TokenType::In);
        consume(TokenType::LeftCurlyBrace);

        _variableStack.push(variableListId);
        auto inExprId = parseInternal(0);
        _variableStack.pop();

        auto [letExprId, letExpr] = _pool.allocate(ExpressionType::Let);
        letExpr.pushChild(variableListId);
        letExpr.pushChild(inExprId);

        consume(TokenType::RightCurlyBrace);

        return letExprId;
    }

    constexpr ExpressionId consumeIf(TokenType startToken = TokenType::If) {
        consume(startToken);
        auto conditionExprId = parseInternal(0);
        consume(TokenType::LeftCurlyBrace);
        auto thenExprId = parseInternal(0);
        consume(TokenType::RightCurlyBrace);

        ExpressionId elseExprId = 0;
        if (match(TokenType::Else)) {
            consume(TokenType::Else);
            consume(TokenType::LeftCurlyBrace);
            elseExprId = parseInternal(0);
            consume(TokenType::RightCurlyBrace);
        } else {
            elseExprId = consumeIf(TokenType::Elif);
        }

        auto [ifExprId, ifExpr] = _pool.allocate(ExpressionType::If);
        ifExpr.pushChild(conditionExprId);
        ifExpr.pushChild(thenExprId);
        ifExpr.pushChild(elseExprId);
        return ifExprId;
    }

    constexpr ExpressionId consumeBoolean() {
        auto [exprId, _] = _pool.allocate(_current.data.boolean);
        consume(TokenType::Boolean);
        return exprId;
    }

    constexpr ExpressionId consumeString() {
        auto [exprId, _] = _pool.allocate(ExpressionType::String, _current.data.string);
        consume(TokenType::String);
        return exprId;
    }

    constexpr ExpressionId consumeInteger() {
        ExpressionId exprId = 0;
        if (_current.data.integer.is64Bit) {
            auto [allocatedId, _] = _pool.allocate(_current.data.integer.value);
            exprId = allocatedId;
        } else {
            auto [allocatedId, _] =
                _pool.allocate(static_cast<int32_t>(_current.data.integer.value));
            exprId = allocatedId;
        }
        consume(TokenType::Integer);
        return exprId;
    }

    constexpr ExpressionId consumeKeyword() {
        auto [exprId, _] = _pool.allocate(getKeywordType(peek()));
        advance();
        return exprId;
    }

    constexpr std::tuple<value::SlotId, uint64_t, bool> lookupVariableByName(
        std::string_view name) const {
        for (const auto& variableListId : _variableStack) {
            auto currentVariableId = variableListId;
            while (true) {
                auto& currentVariable = _pool.get(currentVariableId);
                if (currentVariable.stringValue == name) {
                    return {currentVariable.variableId, currentVariable.frameIndex, true};
                }

                if (currentVariable.childrenCount < 2) {
                    break;
                }

                currentVariableId = currentVariable.children[1];
            }
        }

        return {0, 0, false};
    }

    constexpr ExpressionId createVariable(std::string_view name) {
        auto [slotId, frameIndex, isDefined] = lookupVariableByName(name);

        if (!isDefined) {
            throw std::logic_error("Undefined variable");
        }

        auto [exprId, _] = _pool.allocate(slotId, frameIndex);
        return exprId;
    }

    constexpr ExpressionId consumeVariableOrFunctionCall() {
        auto name = _current.data.string;
        consume(TokenType::Identifier);

        if (!match(TokenType::LeftParen)) {
            return createVariable(name);
        }

        auto [exprId, expr] = _pool.allocate(ExpressionType::FunctionCall, name);
        consume(TokenType::LeftParen);

        if (name == "fail") {
            consumeFailArguments(expr);
        } else if (name == "nullOrMissing") {
            consumeNullOrMissingArguments(expr);
        } else if (name == "toInt32") {
            consumeArguments(expr, 1);
        } else {
            consumeArguments(expr);
        }

        consume(TokenType::RightParen);
        return exprId;
    }

    constexpr void consumeNullOrMissingArguments(Expression& expr) {
        if (!match(TokenType::Identifier)) {
            throw std::logic_error("nullOrMissing argument must be a variable");
        }
        auto name = _current.data.string;
        consume(TokenType::Identifier);
        expr.pushChild(createVariable(name));
    }

    constexpr void consumeFailArguments(Expression& expr) {
        if (!match(TokenType::Integer)) {
            throw std::logic_error("First argument of fail() must me error code (32-bit integer)");
        }
        expr.pushChild(consumeInteger());

        consume(TokenType::Comma);

        if (!match(TokenType::String)) {
            throw std::logic_error("Second argument of fail() must me error message string");
        }
        expr.pushChild(consumeString());
    }

    constexpr void consumeArguments(Expression& expr,
                                    uint64_t maxArgumentsCount = Expression::MAX_CHILDREN) {
        bool isFirst = true;
        while (!match(TokenType::RightParen) && expr.childrenCount < maxArgumentsCount) {
            if (!isFirst) {
                consume(TokenType::Comma);
            }
            isFirst = false;
            ExpressionId childId = parseInternal(0);
            expr.pushChild(childId);
        }
    }

    constexpr void advance() {
        _current = _tokenizer.next();
    }

    constexpr bool match(TokenType type) {
        return _current.type == type;
    }

    constexpr TokenType peek() {
        return _current.type;
    }

    constexpr void consume(TokenType type) {
        if (!match(type)) {
            throw std::logic_error("Unexpected token");
        }
        advance();
    }

    Tokenizer _tokenizer;
    Token _current;
    ExpressionPool _pool;
    ExpressionIdStack _variableStack;
    uint64_t currentFrameIndex;
};

constexpr ExpressionPool operator""_sbe(const char* str, size_t length) {
    return Parser(std::string_view(str, length)).parse();
}

}  // namespace mongo::sbe::ctp