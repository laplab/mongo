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

#include <string_view>
#include <stdexcept>

namespace mongo::sbe::ctp {

enum class TokenType {
    Eof,
    LeftParen,
    RightParen,
    Comma,
    Identifier,
    Nothing,
    Null,
    Integer,
    Boolean,
    And,
    Or,
    If,
    Else,
    LeftCurlyBrace,
    RightCurlyBrace,
};

constexpr bool isOperator(TokenType type) {
    return type == TokenType::Or || type == TokenType::And;
}

struct Token {
    TokenType type;

    union {
        std::string_view name;
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
        return Token{type, {.boolean = false}};
    }

    static constexpr Token punctuation(TokenType type) {
        return Token{type, {.boolean = false}};
    }

    static constexpr Token eof() {
        return Token{TokenType::Eof, {.boolean = false}};
    }
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
            case '{':
                type = TokenType::LeftCurlyBrace;
                break;
            case '}':
                type = TokenType::RightCurlyBrace;
                break;
            case '&': {
                advance();
                if (peek() != '&') {
                    throw std::logic_error("Incomplete AND operator");
                }
                type = TokenType::And;
                break;
            }
            case '|': {
                advance();
                if (peek() != '|') {
                    throw std::logic_error("Incomplete OR operator");
                }
                type = TokenType::Or;
                break;
            }
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
        } else if (identifier == "if") {
            return Token::keyword(TokenType::If);
        } else if(identifier == "else") {
            return Token::keyword(TokenType::Else);
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

}