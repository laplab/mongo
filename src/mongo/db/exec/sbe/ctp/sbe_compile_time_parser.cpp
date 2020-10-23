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

#include "sbe_compile_time_parser.h"

namespace mongo::sbe::ctp {

std::unique_ptr<sbe::EExpression> Expression::build(const ExpressionPool& exprs, BuildContext& context) const {
    switch (type) {
        case ExpressionType::Placeholder:
            return std::move(context.indexedPlaceholders[data.index]);

        case ExpressionType::FunctionCall: {
            auto compiledChildren = sbe::makeEs();
            compiledChildren.reserve(childrenCount);

            for (uint64_t i = 0; i < childrenCount; i++) {
                auto compiledChild = exprs.get(children[i]).build(exprs, context);
                compiledChildren.emplace_back(std::move(compiledChild));
            }
            return sbe::makeE<sbe::EFunction>(data.name, std::move(compiledChildren));
        }

        case ExpressionType::None:
            MONGO_UNREACHABLE;
    }

    return nullptr;
}

}