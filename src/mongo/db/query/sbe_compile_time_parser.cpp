#include "sbe_compile_time_parser.h"

namespace mongo::sbe_ctp {

std::unique_ptr<sbe::EExpression> Expression::build(BuildContext& context) const {
    switch (type) {
        case ExpressionType::Placeholder:
            return std::move(context.placeholderValues[data.index]);

        case ExpressionType::FunctionCall: {
            auto compiledChildren = sbe::makeEs();
            compiledChildren.reserve(childrenCount);

            for (long long i = 0; i < childrenCount; i++) {
                auto compiledChild = context.result.get(children[i]).build(context);
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