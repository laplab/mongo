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

#include "mongo/db/exec/sbe/ctp/tokenizer.h"
#include "mongo/db/exec/sbe/ctp/expression.h"

namespace mongo::sbe::ctp {

class Parser {
public:
    constexpr Parser(std::string_view input)
        : _tokenizer(input), _current(_tokenizer.next()), _pool() {}

    constexpr ExpressionPool parse() {
        parseInternal();
        consume(TokenType::Eof);
        return _pool;
    }

private:
    constexpr ExpressionId parseInternal() {
        switch (peek()) {
            case TokenType::Placeholder:
                return consumePlaceholder();
            case TokenType::Identifier:
                return consumeFunctionCall();
            case TokenType::Nothing:
            case TokenType::Null:
                return consumeKeyword();
            case TokenType::Integer:
                return consumeInteger();
            case TokenType::Boolean:
                return consumeBoolean();
            default:
                throw std::logic_error("Unexpected token type");
        }
    }

    constexpr ExpressionId consumePlaceholder() {
        ExpressionId exprIndex = _pool.allocate();
        _pool.get(exprIndex) = Expression(_current.data.index);
        consume(TokenType::Placeholder);
        return exprIndex;
    }

    constexpr ExpressionId consumeBoolean() {
        ExpressionId exprIndex = _pool.allocate();
        _pool.get(exprIndex) = Expression(_current.data.boolean);
        consume(TokenType::Boolean);
        return exprIndex;
    }

    constexpr ExpressionId consumeInteger() {
        ExpressionId exprIndex = _pool.allocate();
        if (_current.data.integer.is64Bit) {
            _pool.get(exprIndex) = Expression{_current.data.integer.value};
        } else {
            _pool.get(exprIndex) = Expression{static_cast<int32_t>(_current.data.integer.value)};
        }
        consume(TokenType::Integer);
        return exprIndex;
    }

    constexpr ExpressionId consumeKeyword() {
        ExpressionType type = ExpressionType::None;
        switch (peek()) {
            case TokenType::Nothing:
                type = ExpressionType::Nothing;
                break;
            case TokenType::Null:
                type = ExpressionType::Null;
                break;
            default:
                throw std::logic_error("Expected keyword token type");
        }

        ExpressionId exprIndex = _pool.allocate();
        _pool.get(exprIndex) = Expression(type);
        advance();
        return exprIndex;
    }

    constexpr ExpressionId consumeFunctionCall() {
        std::string_view functionName = _current.data.name;

        ExpressionId exprIndex = _pool.allocate();
        _pool.get(exprIndex) = Expression(functionName);

        consume(TokenType::Identifier);
        consume(TokenType::LeftParen);

        bool isFirst = true;
        while (!match(TokenType::RightParen)) {
            if (!isFirst) {
                consume(TokenType::Comma);
            }
            isFirst = false;
            ExpressionId childIndex = parseInternal();
            _pool.get(exprIndex).pushChild(childIndex);
        }

        consume(TokenType::RightParen);
        return exprIndex;
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