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
#include <stack>
#include <unordered_map>

#include "mongo/db/exec/sbe/expressions/expression.h"

namespace mongo::sbe::ctp {

static constexpr const char* SKUNK_STRING = R"(
  _.--.
.'   ` '
``'.  .'     .c-..
    `.  ``````  .-'
-'`. )--. .'`
`-`._   \_`--
)";

enum class ExpressionType {
    None,
    FunctionCall,
    Placeholder,
    Nothing,
    Null,
    Int32,
    Int64,
    Boolean,
    And,
    Or,
    If,
    VariableAssignment,
    Let,
    Variable,
    String,
    Not,
    Skunk,
    Add,
    Subtract,
};

using ExpressionId = uint64_t;
class ExpressionPool;

struct VariablesFrame {
    FrameId frameId;
    std::vector<std::unique_ptr<EExpression>> binds;
};

struct BuildContext {
    BuildContext(value::FrameIdGenerator& frameIdGenerator,
                 std::vector<std::unique_ptr<sbe::EExpression>>& indexedPlaceholders)
        : frameIdGenerator(frameIdGenerator), indexedPlaceholders(indexedPlaceholders) {}

    value::FrameIdGenerator& frameIdGenerator;
    std::vector<VariablesFrame> variableStack;
    std::vector<std::unique_ptr<sbe::EExpression>>& indexedPlaceholders;
};

struct Expression {
    constexpr Expression()
        : type(ExpressionType::None),
          placeholderIndex(0),
          stringValue(""),
          int32Value(0),
          int64Value(0),
          boolValue(false),
          variableId(0),
          frameIndex(0),
          childrenCount(0),
          children() {}

    explicit constexpr Expression(ExpressionType exprType) : Expression() {
        type = exprType;
    }

    template <typename T>
    explicit constexpr Expression(T value) : Expression() {
        if constexpr (std::is_same_v<T, bool>) {
            type = ExpressionType::Boolean;
            boolValue = value;
        } else if constexpr (std::is_same_v<T, int32_t>) {
            type = ExpressionType::Int32;
            int32Value = value;
        } else if constexpr (std::is_same_v<T, int64_t>) {
            type = ExpressionType::Int64;
            int64Value = value;
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            type = ExpressionType::Placeholder;
            placeholderIndex = value;
        } else {
            throw std::logic_error("Unexpected type");
        }
    }

    explicit constexpr Expression(value::SlotId slotId, uint64_t frameId) : Expression() {
        type = ExpressionType::Variable;
        variableId = slotId;
        frameIndex = frameId;
    }

    explicit constexpr Expression(std::string_view name, value::SlotId slotId, uint64_t frameId)
        : Expression() {
        type = ExpressionType::VariableAssignment;
        stringValue = name;
        variableId = slotId;
        frameIndex = frameId;
    }

    explicit constexpr Expression(ExpressionType exprType, std::string_view name) : Expression() {
        type = exprType;
        stringValue = name;
    }

    constexpr void pushChild(ExpressionId childIndex) {
        children[childrenCount++] = childIndex;
    }

    std::unique_ptr<sbe::EExpression> build(const ExpressionPool& exprs,
                                            BuildContext& context) const;

    ExpressionType type;
    uint64_t placeholderIndex;
    std::string_view stringValue;
    int32_t int32Value;
    int64_t int64Value;
    bool boolValue;
    value::SlotId variableId;
    uint64_t frameIndex;

    uint64_t childrenCount;
    static constexpr long long MAX_CHILDREN = 3;
    ExpressionId children[MAX_CHILDREN];
};

class ExpressionPool {
public:
    template <typename... Args>
    auto operator()(value::FrameIdGenerator& frameIdGenerator, Args&&... args) const {
        std::vector<std::unique_ptr<sbe::EExpression>> indexedPlaceholders;
        indexedPlaceholders.reserve(sizeof...(Args));
        (indexedPlaceholders.push_back(std::forward<Args>(args)), ...);

        BuildContext context{frameIdGenerator, indexedPlaceholders};
        return getRoot().build(*this, context);
    }

    constexpr const Expression& getRoot() const {
        return pool[rootId];
    }

    constexpr const Expression& get(ExpressionId index) const {
        return pool[index];
    }

    template <typename... Args>
    constexpr std::pair<ExpressionId, Expression&> allocate(Args&&... args) {
        if (current == MAX_SIZE) {
            throw std::logic_error("Not enough space, increase MAX_SIZE constant.");
        }
        ExpressionId index = current;
        current++;
        pool[index] = Expression{std::forward<Args>(args)...};
        return {index, pool[index]};
    }

    constexpr void setRootId(ExpressionId exprId) {
        rootId = exprId;
    }

private:
    ExpressionId rootId = 0;
    static constexpr long long MAX_SIZE = 100;
    Expression pool[MAX_SIZE];
    ExpressionId current = 0;
};

}  // namespace mongo::sbe::ctp