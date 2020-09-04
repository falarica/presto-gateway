package io.prestosql.gateway.persistence.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import io.prestosql.gateway.routing.RoutingPolicySpec;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.IdName;
import org.javalite.activejdbc.annotations.Table;

import java.util.List;
import java.util.stream.Collectors;

@Table("routingpolicy")
@IdName("name")
public class RoutingPolicy
        extends Model
{
    private static final String name = "name";
    private static final String ruleIds = "ruleids";

    public static List<RoutingPolicySpec> upcast(List<RoutingPolicy> policies)
    {
        Logger log = Logger.get(RoutingPolicy.class);
        return policies.stream().map(policy -> {
            String jsonArrayString = policy.getString("ruleids");
            ObjectMapper objectMapper = new ObjectMapper();
            List<String> ruleIdsList = null;
            try {
                ruleIdsList = objectMapper.readValue(jsonArrayString, new TypeReference<List<String>>() {});
            }
            catch (JsonProcessingException e) {
                log.warn(e, "Exception while decosing JSON");
            }

            return new RoutingPolicySpec(policy.getString("name"),
                    ruleIdsList);
        }).collect(Collectors.toList());
    }

    public static RoutingPolicy create(RoutingPolicySpec info)
            throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonString = objectMapper.writeValueAsString(info.getRoutingRules().toArray());

        RoutingPolicy p = RoutingPolicy.create(name, info.getName(),
                ruleIds, jsonString);
        p.insert();
        return p;
    }

    public static RoutingPolicy update(RoutingPolicy policy, RoutingPolicySpec info)
            throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonString = objectMapper.writeValueAsString(info.getRoutingRules().toArray());
        RoutingPolicy p = policy.set(name, info.getName(),
                ruleIds, jsonString);
        p.saveIt();
        return p;
    }
}
