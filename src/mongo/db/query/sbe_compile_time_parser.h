#pragma once

#include <utility>
#include <string_view>
#include <stdexcept>
#include <sstream>
#include <string>
#include <vector>

#include "mongo/db/exec/sbe/expressions/expression.h"

namespace mongo::sbe_ctp {

enum class TokenType {
    Placeholder,
    Eof,
    LeftParen,
    RightParen,
    Comma,
    Identifier,
};

struct Token {
    TokenType type;

    union {
        std::string_view name;
        long long index;
    } data;

    static constexpr Token identifer(std::string_view name) {
        return Token{TokenType::Identifier, {.name = name}};
    }

    static constexpr Token punctuation(TokenType type) {
        return Token{type, {.index = 0}};
    }

    static constexpr Token placeholder(long long index) {
        return Token{TokenType::Placeholder, {.index = index}};
    }

    static constexpr Token eof() {
        return Token{TokenType::Eof, {.index = 0}};
    }

    std::string toString() {
        std::stringstream result;
        result << "Token(";

        switch (type) {
            case TokenType::Placeholder:
                result << "Placeholder, {" << data.index << "}";
                break;

            case TokenType::Eof:
                result << "EOF";
                break;

            case TokenType::LeftParen:
                result << "LeftParen";
                break;

            case TokenType::RightParen:
                result << "RightParen";
                break;

            case TokenType::Comma:
                result << "Comma";
                break;

            case TokenType::Identifier:
                result << "Identifier, '" << data.name << "'";
                break;
        };

        result << ")";
        return result.str();
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

        if (next == '{') {
            return consumePlaceholder();
        }

        if (isAlpha(next)) {
            return consumeIdentifier();
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
    constexpr Token consumePlaceholder() {
        consume('{');

        long long index = 0;
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

    constexpr Token consumeIdentifier() {
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
        return Token::identifer(std::string_view(start, length));
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
};

using ExpressionId = uint64_t;
struct ExpressionPool;

struct BuildContext {
    std::vector<std::unique_ptr<sbe::EExpression>>& indexedPlaceholders;
};

struct Expression {
    constexpr Expression()
        : type(ExpressionType::None), data({.index = 0}), children(), childrenCount(0) {}

    constexpr Expression(ExpressionId index)
        : type(ExpressionType::Placeholder),
          data({.index = index}),
          children(),
          childrenCount(0) {}

    constexpr Expression(std::string_view name)
        : type(ExpressionType::FunctionCall), data({.name = name}), children(), childrenCount(0) {}

    constexpr void pushChild(ExpressionId childIndex) {
        children[childrenCount++] = childIndex;
    }

    std::unique_ptr<sbe::EExpression> build(const ExpressionPool& exprs, BuildContext& context) const;

    ExpressionType type;
    union {
        ExpressionId index;
        std::string_view name;
    } data;

    static constexpr long long MAX_CHILDREN = 2;
    ExpressionId children[MAX_CHILDREN];
    uint64_t childrenCount;
};

struct ExpressionPool {
    std::unique_ptr<sbe::EExpression> build(std::vector<std::unique_ptr<sbe::EExpression>>& indexedPlaceholders) const {
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
        return _pool;
    }

private:
    constexpr ExpressionId compileInternal() {
        if (match(TokenType::Placeholder)) {
            return consumePlaceholder();
        }

        if (match(TokenType::Identifier)) {
            return consumeFunctionCall();
        }

        throw std::logic_error("Unexpected token");
    }

    constexpr ExpressionId consumePlaceholder() {
        ExpressionId exprIndex = _pool.allocate();
        _pool.get(exprIndex) = Expression(_current.data.index);
        consume(TokenType::Placeholder);
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