package io.prestosql.gateway.persistence.dao;

import io.prestosql.gateway.routing.RoutingPolicySelectorSpec;
import org.javalite.activejdbc.Model;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RoutingPolicySelector
        extends Model
{
    private static final String name = "name";
    private static final String userRegex = "userRegex";
    private static final String userGroupRegex = "userGroupRegex";
    private static final String sourceRegex = "sourceRegex";
    private static final String queryType = "queryType";
    private static final String clientTags = "clientTags";

    public static List<RoutingPolicySelectorSpec> upcast(List<RoutingPolicySelector> policySelectors)
    {
        return policySelectors.stream().map(selector -> {
            List<RoutingPolicySelector> c = selector.getAll(RoutingPolicySelector.class);
            String[] cTags = selector.getString("clientTags").split(",");
            return new RoutingPolicySelectorSpec(selector.getString("name"),
                    Optional.ofNullable(Pattern.compile(selector.getString("userRegex"))),
                    Optional.ofNullable(Pattern.compile(selector.getString("userGroupRegex"))),
                    Optional.ofNullable(Pattern.compile(selector.getString("sourceRegex"))),
                    Optional.ofNullable(selector.getString("queryType")),
                    Optional.ofNullable(Arrays.asList(cTags)));
        }).collect(Collectors.toList());
    }

    public static void create(RoutingPolicySelectorSpec spec)
    {
        // insert spec
        final String[] userRegEx = new String[1];
        spec.getUserRegex().ifPresentOrElse(
                d -> { userRegEx[0] = d.toString(); }, () -> { userRegEx[0] = null; });

        final String[] uGroupRegEx = new String[1];
        spec.getUserGroupRegex().ifPresentOrElse(
                d -> { uGroupRegEx[0] = d.toString(); }, () -> { uGroupRegEx[0] = null; });

        final String[] sourceRegEx = new String[1];
        spec.getSourceRegex().ifPresentOrElse(
                d -> { sourceRegEx[0] = d.toString(); }, () -> { sourceRegEx[0] = null; });

        final String[] qType = new String[1];
        spec.getQueryType().ifPresentOrElse(
                d -> { qType[0] = d.toString(); }, () -> { qType[0] = null; });

        final String[] cTags = new String[1];
        spec.getClientTags().ifPresentOrElse(
                d -> { cTags[0] = d.toString(); }, () -> { cTags[0] = null; });

        RoutingPolicySelector p = RoutingPolicySelector.create(name, spec.getName(),
                userGroupRegex, uGroupRegEx[0],
                userRegex, userRegEx[0],
                sourceRegex, sourceRegEx[0],
                queryType, qType[0],
                clientTags, cTags[0]);

        p.insert();
    }
}
