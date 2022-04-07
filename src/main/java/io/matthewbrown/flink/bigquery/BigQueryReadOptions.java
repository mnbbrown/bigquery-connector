package io.matthewbrown.flink.bigquery;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.*;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BigQueryReadOptions {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryReadOptions.class);

    Long limit = -1L;
    List<String> filters = new ArrayList<>();

    public Long getLimit() {
        return limit;
    }

    public void setLimit(Long limit) {
        this.limit = limit;
    }

    public List<String> getFilters() {
        return filters;
    }

    private static final ImmutableMap<FunctionDefinition, Converter> CONVERTERS = new ImmutableMap.Builder<FunctionDefinition, Converter>()
            .put(BuiltInFunctionDefinitions.EQUALS, expression -> {
                List<Expression> children = expression.getChildren();
                Expression left = children.get(0);
                Expression right = children.get(1);
                return left.asSummaryString() + " = " + right.asSummaryString();
            })
            .put(BuiltInFunctionDefinitions.NOT_EQUALS, expression -> {
                List<Expression> children = expression.getChildren();
                Expression left = children.get(0);
                Expression right = children.get(1);
                return left.asSummaryString() + " != " + right.asSummaryString();
            })
            .build();

    @FunctionalInterface
    public interface Converter extends Serializable {
        String convert(CallExpression input);
    }

    public SupportsFilterPushDown.Result setFilters(List<ResolvedExpression> filters) {
        List<ResolvedExpression> unapplied = new ArrayList<>();
        List<ResolvedExpression> applied = new ArrayList<>();

        for (ResolvedExpression filter : filters) {
            if (filter instanceof CallExpression) {
                CallExpression callExpression = (CallExpression) filter;
                FunctionDefinition functionDefinition = callExpression.getFunctionDefinition();
                Converter convert = CONVERTERS.get(functionDefinition);
                if (convert != null) {
                    LOG.warn("converter not available for " + callExpression);
                    String converted = convert.convert(callExpression);
                    if (converted != null) {
                        LOG.warn("unable to convert expression to " + callExpression);
                        this.filters.add(convert.convert(callExpression));
                        applied.add(filter);
                    } else {
                        unapplied.add(filter);
                    }
                } else {
                    unapplied.add(filter);
                }
            } else {
                unapplied.add(filter);
            }
        }
        return SupportsFilterPushDown.Result.of(applied, unapplied);
    }
}
