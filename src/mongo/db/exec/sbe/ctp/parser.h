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

#include <utility>
#include <string_view>
#include <stdexcept>
#include <sstream>
#include <string>
#include <vector>
#include <cstdint>

#include "mongo/db/exec/sbe/ctp/tokenizer.h"
#include "mongo/db/exec/sbe/ctp/expression.h"

namespace mongo::sbe::ctp {

constexpr ExpressionType getOperatorType(TokenType type) {
    switch (type) {
        case TokenType::And:
            return ExpressionType::And;
        case TokenType::Or:
            return ExpressionType::Or;
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
        default:
            throw std::logic_error("Expected operator token type");
    }
}

class Parser {
public:
    constexpr Parser(std::string_view input)
        : _tokenizer(input), _current(_tokenizer.next()), _pool() {}

    constexpr ExpressionPool parse() {
        _pool.rootId = parseInternal(0);
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

            auto opExprId = _pool.allocate();
            Expression& opExpr = _pool.get(opExprId);
            opExpr = Expression(getOperatorType(type));
            opExpr.pushChild(leftExprId);
            opExpr.pushChild(rightExprId);

            leftExprId = opExprId;
        }

        return leftExprId;
    }

    constexpr ExpressionId consumePlaceholder() {
        consume(TokenType::LeftCurlyBrace);
        if (_current.data.integer.is64Bit) {
            throw std::logic_error("Only 32-bit integer literals are supported as placeholder indexes");
        }
        ExpressionId exprId = _pool.allocate();
        _pool.get(exprId) = Expression{static_cast<uint64_t>(_current.data.integer.value)};
        consume(TokenType::Integer);
        consume(TokenType::RightCurlyBrace);
        return exprId;
    }

    constexpr ExpressionId consumeLet() {
        consume(TokenType::Let);

        ExpressionId variableTreeId = 0;
        bool isFirst = true;
        while (true) {
            if (match(TokenType::Comma)) {
                consume(TokenType::Comma);
            } else if (!isFirst) {
                break;
            }

            auto variableName = _current.data.name;
            consume(TokenType::Identifier);
            consume(TokenType::Equals);

            auto variableValue = parseInternal(0);

            auto variableExprId = _pool.allocate();
            Expression& variable = _pool.get(variableExprId);
            variable = Expression(ExpressionType::VariableAssignment, variableName);
            variable.pushChild(variableValue);

            if (!isFirst) {
                variable.pushChild(variableTreeId);
            }
            variableTreeId = variableExprId;

            isFirst = false;
        }

        consume(TokenType::In);
        consume(TokenType::LeftCurlyBrace);
        auto inExprId = parseInternal(0);

        auto letExprId = _pool.allocate();
        Expression& letExpr = _pool.get(letExprId);
        letExpr = Expression(ExpressionType::Let);
        letExpr.pushChild(variableTreeId);
        letExpr.pushChild(inExprId);

        consume(TokenType::RightCurlyBrace);

        return letExprId;
    }

    constexpr ExpressionId consumeIf() {
        consume(TokenType::If);
        auto conditionExprId = parseInternal(0);
        consume(TokenType::LeftCurlyBrace);
        auto thenExprId = parseInternal(0);
        consume(TokenType::RightCurlyBrace);
        consume(TokenType::Else);
        consume(TokenType::LeftCurlyBrace);
        auto elseExprId = parseInternal(0);
        consume(TokenType::RightCurlyBrace);

        ExpressionId ifExprId= _pool.allocate();
        Expression& ifExpr = _pool.get(ifExprId);
        ifExpr = Expression(ExpressionType::If);
        ifExpr.pushChild(conditionExprId);
        ifExpr.pushChild(thenExprId);
        ifExpr.pushChild(elseExprId);
        return ifExprId;
    }

    constexpr ExpressionId consumeBoolean() {
        ExpressionId exprId = _pool.allocate();
        _pool.get(exprId) = Expression(_current.data.boolean);
        consume(TokenType::Boolean);
        return exprId;
    }

    constexpr ExpressionId consumeInteger() {
        ExpressionId exprId = _pool.allocate();
        if (_current.data.integer.is64Bit) {
            _pool.get(exprId) = Expression{_current.data.integer.value};
        } else {
            _pool.get(exprId) = Expression{static_cast<int32_t>(_current.data.integer.value)};
        }
        consume(TokenType::Integer);
        return exprId;
    }

    constexpr ExpressionId consumeKeyword() {
        ExpressionId exprId = _pool.allocate();
        _pool.get(exprId) = Expression(getKeywordType(peek()));
        advance();
        return exprId;
    }

    constexpr ExpressionId consumeVariableOrFunctionCall() {
        auto name = _current.data.name;
        consume(TokenType::Identifier);

        ExpressionId exprId = _pool.allocate();
        Expression& expr = _pool.get(exprId);
        if (!match(TokenType::LeftParen)) {
            expr = Expression(ExpressionType::Variable, name);
            return exprId;
        }

        expr = Expression(ExpressionType::FunctionCall, name);
        consume(TokenType::LeftParen);

        bool isFirst = true;
        while (!match(TokenType::RightParen)) {
            if (!isFirst) {
                consume(TokenType::Comma);
            }
            isFirst = false;
            ExpressionId childId = parseInternal(0);
            _pool.get(exprId).pushChild(childId);
        }

        consume(TokenType::RightParen);
        return exprId;
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
};

constexpr ExpressionPool operator""_sbe(const char* str, size_t length) {
    return Parser(std::string_view(str, length)).parse();
}

}