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

#include "mongo/db/exec/sbe/expressions/expression.h"

namespace mongo::sbe::ctp {

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

}