package io.prestosql.gateway.routing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RoutingPolicySelectorSpec
{
    private final String name;
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> userGroupRegex;
    private final Optional<Pattern> sourceRegex;
    private final Optional<String> queryType;
    private final Optional<List<String>> clientTags;

    @JsonCreator
    public RoutingPolicySelectorSpec(
            @JsonProperty("name") String name,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("userGroup") Optional<Pattern> userGroupRegex,
            @JsonProperty("source") Optional<Pattern> sourceRegex,
            @JsonProperty("queryType") Optional<String> queryType,
            @JsonProperty("clientTags") Optional<List<String>> clientTags)
    {
        this.name = name;
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.userGroupRegex = requireNonNull(userGroupRegex, "userGroupRegex is null");
        this.sourceRegex = requireNonNull(sourceRegex, "sourceRegex is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.clientTags = requireNonNull(clientTags, "clientTags is null");
    }

    @JsonProperty
    public String getName()
    {
        return this.name;
    }

    @JsonProperty
    public Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }

    @JsonProperty
    public Optional<Pattern> getUserGroupRegex()
    {
        return userGroupRegex;
    }

    @JsonProperty
    public Optional<Pattern> getSourceRegex()
    {
        return sourceRegex;
    }

    @JsonProperty
    public Optional<String> getQueryType()
    {
        return queryType;
    }

    @JsonProperty
    public Optional<List<String>> getClientTags()
    {
        return clientTags;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof RoutingPolicySelectorSpec)) {
            return false;
        }
        RoutingPolicySelectorSpec that = (RoutingPolicySelectorSpec) other;
        return (
                userRegex.map(Pattern::pattern).equals(that.userRegex.map(Pattern::pattern)) &&
                        userRegex.map(Pattern::flags).equals(that.userRegex.map(Pattern::flags)) &&
                        userGroupRegex.map(Pattern::pattern).equals(that.userGroupRegex.map(Pattern::pattern)) &&
                        userGroupRegex.map(Pattern::flags).equals(that.userGroupRegex.map(Pattern::flags)) &&
                        sourceRegex.map(Pattern::pattern).equals(that.sourceRegex.map(Pattern::pattern))) &&
                sourceRegex.map(Pattern::flags).equals(that.sourceRegex.map(Pattern::flags)) &&
                queryType.equals(that.queryType) &&
                clientTags.equals(that.clientTags);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                userRegex.map(Pattern::pattern),
                userRegex.map(Pattern::flags),
                userGroupRegex.map(Pattern::pattern),
                userGroupRegex.map(Pattern::flags),
                sourceRegex.map(Pattern::pattern),
                sourceRegex.map(Pattern::flags),
                queryType,
                clientTags);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("userRegex", userRegex)
                .add("userFlags", userRegex.map(Pattern::flags))
                .add("userGroupRegex", userGroupRegex)
                .add("userGroupFlags", userGroupRegex.map(Pattern::flags))
                .add("sourceRegex", sourceRegex)
                .add("sourceFlags", sourceRegex.map(Pattern::flags))
                .add("queryType", queryType)
                .add("clientTags", clientTags)
                .toString();
    }
}
