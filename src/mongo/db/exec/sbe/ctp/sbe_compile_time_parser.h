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

#include "mongo/db/exec/sbe/expressions/expression.h"

namespace mongo::sbe::ctp {

enum class TokenType {
    Placeholder,
    Eof,
    LeftParen,
    RightParen,
    Comma,
    Identifier,
    Nothing,
    Null,
    Integer,
    Boolean,
};

struct Token {
    TokenType type;

    union {
        std::string_view name;
        uint64_t index;
        struct {
            int64_t value;
            bool is64Bit;
        } integer;
        bool boolean;
    } data;

    static constexpr Token boolean(bool value) {
        return Token{TokenType::Boolean, {.boolean = value}};
    }

    static constexpr Token integer(int64_t value, bool is64Bit) {
        return Token{TokenType::Integer, {.integer = {value, is64Bit}}};
    }

    static constexpr Token identifer(std::string_view name) {
        return Token{TokenType::Identifier, {.name = name}};
    }

    static constexpr Token keyword(TokenType type) {
        return Token{type, {.index = 0}};
    }

    static constexpr Token punctuation(TokenType type) {
        return Token{type, {.index = 0}};
    }

    static constexpr Token placeholder(uint64_t index) {
        return Token{TokenType::Placeholder, {.index = index}};
    }

    static constexpr Token eof() {
        return Token{TokenType::Eof, {.index = 0}};
    }

    // std::string toString() {
    //     std::stringstream result;
    //     result << "Token(";

    //     switch (type) {
    //         case TokenType::Placeholder:
    //             result << "Placeholder, {" << data.index << "}";
    //             break;

    //         case TokenType::Eof:
    //             result << "EOF";
    //             break;

    //         case TokenType::LeftParen:
    //             result << "LeftParen";
    //             break;

    //         case TokenType::RightParen:
    //             result << "RightParen";
    //             break;

    //         case TokenType::Comma:
    //             result << "Comma";
    //             break;

    //         case TokenType::Identifier:
    //             result << "Identifier, '" << data.name << "'";
    //             break;
    //     };

    //     result << ")";
    //     return result.str();
    // }
};

namespace {

constexpr bool isDigit(char c) {
    return c >= '0' && c <= '9';
}

constexpr bool isAlpha(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

constexpr bool isWhitespace(char c) {
    return c == ' ' || c == '\t' || c == '\n';
}

}

class Tokenizer {
public:
    constexpr Tokenizer(std::string_view input)
        : _input(input)
    {
    }

    constexpr Token next() {
        trimWhitespace();
        if (atEnd()) {
            return Token::eof();
        }

        char next = peek();

        if (next == '{') {
            return consumePlaceholder();
        }

        if (isAlpha(next)) {
            return consumeIdentifierOrKeyword();
        }

        if (isDigit(next)) {
            return consumeInteger();
        }

        TokenType type = TokenType::Eof;
        switch (next) {
            case '(':
                type = TokenType::LeftParen;
                break;
            case ')':
                type = TokenType::RightParen;
                break;
            case ',':
                type = TokenType::Comma;
                break;
            default:
                throw std::logic_error("Unexpected character");
        }
        advance();

        return Token::punctuation(type);
    }

private:
    constexpr Token consumeInteger() {
        int64_t value = 0;
        while (true) {
            char current = peek();
            if (!isDigit(current)) {
                break;
            }

            value = value * 10 + (current - '0');

            advance();
        }

        bool is64Bit = false;
        if (peek() == 'l') {
            is64Bit = true;
            advance();
        }

        return Token::integer(value, is64Bit);
    }

    constexpr Token consumePlaceholder() {
        consume('{');

        uint64_t index = 0;
        while (true) {
            char current = peek();
            if (!isDigit(current)) {
                break;
            }

            index = index * 10 + (current - '0');

            advance();
        }

        consume('}');

        return Token::placeholder(index);
    }

    constexpr Token consumeIdentifierOrKeyword() {
        const char* start = _input.data();
        long long length = 0;
        while (true) {
            char current = peek();
            if (!isAlpha(current)) {
                break;
            }

            length++;
            advance();
        }

        std::string_view identifier(start, length);
        if (identifier == "Nothing") {
            return Token::keyword(TokenType::Nothing);
        } else if (identifier == "Null") {
            return Token::keyword(TokenType::Null);
        } else if (identifier == "true") {
            return Token::boolean(true);
        } else if (identifier == "false") {
            return Token::boolean(false);
        }

        return Token::identifer(identifier);
    }

    constexpr void trimWhitespace() {
        while (isWhitespace(peek())) {
            advance();
        }
    }

    constexpr void advance() {
        if (atEnd()) {
            throw std::logic_error("trying to advance when end of input is reached");
        }
        _input.remove_prefix(1);
    }

    constexpr bool atEnd() {
        return _input.length() == 0;
    }

    constexpr char peek() {
        if (atEnd()) {
            return '\0';
        }
        return _input[0];
    }

    constexpr void consume(char c) {
        if (peek() != c) {
            throw std::logic_error("Unexpected character");
        }
        advance();
    }

    std::string_view _input;
};

enum class ExpressionType {
    None,
    FunctionCall,
    Placeholder,
    Nothing,
    Null,
    Int32,
    Int64,
    Boolean,
};

using ExpressionId = uint64_t;
struct ExpressionPool;

struct BuildContext {
    std::vector<std::unique_ptr<sbe::EExpression>>& indexedPlaceholders;
};

struct Expression {
    constexpr Expression()
        : Expression(ExpressionType::None) {}

    explicit constexpr Expression(bool value)
        : type(ExpressionType::Boolean), data({.boolean = value}), children(), childrenCount(0) {}

    explicit constexpr Expression(int64_t value)
        : type(ExpressionType::Int64), data({.int64Value = value}), children(), childrenCount(0) {}

    explicit constexpr Expression(int32_t value)
        : type(ExpressionType::Int32), data({.int32Value = value}), children(), childrenCount(0) {}

    explicit constexpr Expression(ExpressionType type)
        : type(type), data({.index = 0}), children(), childrenCount(0) {}

    explicit constexpr Expression(uint64_t index)
        : type(ExpressionType::Placeholder),
          data({.index = index}),
          children(),
          childrenCount(0) {}

    explicit constexpr Expression(std::string_view name)
        : type(ExpressionType::FunctionCall), data({.name = name}), children(), childrenCount(0) {}

    constexpr void pushChild(ExpressionId childIndex) {
        children[childrenCount++] = childIndex;
    }

    std::unique_ptr<sbe::EExpression> build(const ExpressionPool& exprs, BuildContext& context) const;

    ExpressionType type;
    union {
        uint64_t index;
        std::string_view name;
        int32_t int32Value;
        int64_t int64Value;
        bool boolean;
    } data;

    static constexpr long long MAX_CHILDREN = 2;
    ExpressionId children[MAX_CHILDREN];
    uint64_t childrenCount;
};

struct ExpressionPool {
    template <typename... Args>
    auto operator()(Args&&... args) const {
        std::vector<std::unique_ptr<sbe::EExpression>> indexedPlaceholders;
        indexedPlaceholders.reserve(sizeof...(Args));
        (indexedPlaceholders.push_back(std::forward<Args>(args)), ...);

        BuildContext context{indexedPlaceholders};
        return getRoot().build(*this, context);
    }

    constexpr const Expression& getRoot() const {
        return pool[0];
    }

    constexpr const Expression& get(ExpressionId index) const {
        return pool[index];
    }

    constexpr Expression& get(ExpressionId index) {
        return pool[index];
    }

    constexpr ExpressionId allocate() {
        if (current == MAX_SIZE) {
            throw std::logic_error("Not enough space, increase MAX_SIZE constant.");
        }
        ExpressionId index = current;
        current++;
        return index;
    }

    static constexpr long long MAX_SIZE = 100;
    Expression pool[MAX_SIZE];
    ExpressionId current = 0;
};

class Compiler {
public:
    constexpr Compiler(std::string_view input)
        : _tokenizer(input), _current(_tokenizer.next()), _pool() {}

    constexpr ExpressionPool compile() {
        compileInternal();
        consume(TokenType::Eof);
        return _pool;
    }

private:
    constexpr ExpressionId compileInternal() {
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
            ExpressionId childIndex = compileInternal();
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
    return Compiler(std::string_view(str, length)).compile();
}

}