package io.prestosql.gateway.persistence.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import io.prestosql.gateway.routing.QueuedQueryRoutingRule;
import io.prestosql.gateway.routing.RandomClusterRoutingRule;
import io.prestosql.gateway.routing.RoundRobinClusterRoutingRule;
import io.prestosql.gateway.routing.RoutingRuleSpec;
import io.prestosql.gateway.routing.RoutingRuleType;
import io.prestosql.gateway.routing.RunningQueryRoutingRule;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.IdName;
import org.javalite.activejdbc.annotations.Table;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Table("routingrules")
@IdName("name")
public class RoutingRuleModel
        extends Model
{
    private static final String name = "name";
    private static final String type = "type";
    private static final String properties = "properties";

    public static void create(RoutingRuleSpec info)
            throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonString = objectMapper.writeValueAsString(info.getProperties());

        RoutingRuleModel p = RoutingRuleModel.create(name, info.getName(),
                type, info.getType(),
                properties, jsonString);
        p.insert();
    }

    public static List<RoutingRuleSpec> upcast(List<RoutingRuleModel> rules)
    {
        Logger log = Logger.get(RoutingRuleModel.class);
        ObjectMapper objectMapper = new ObjectMapper();
        return rules.stream().map(rule -> {
            String ruleType = rule.getString("type");
            RoutingRuleSpec routingRule = null;
            switch (RoutingRuleType.valueOf(ruleType)) {
                case RUNNINGQUERY: {
                    try {
                        routingRule = new RunningQueryRoutingRule(rule.getString("name"),
                                objectMapper.readValue(rule.getString("properties"), new TypeReference<Map<String, String>>() {}));
                    }
                    catch (JsonProcessingException e) {
                        log.warn(e, "Exception while procssing JSON");
                    }
                    break;
                }
                case QUEUEDQUERY: {
                    try {
                        routingRule = new QueuedQueryRoutingRule(rule.getString("name"),
                                objectMapper.readValue(rule.getString("properties"), new TypeReference<Map<String, String>>() {}));
                    }
                    catch (JsonProcessingException e) {
                        log.warn(e, "Exception while decosing JSON.");
                    }
                    break;
                }
                case RANDOMCLUSTER: {
                    routingRule = new RandomClusterRoutingRule();
                    break;
                }
                case ROUNDROBIN: {
                    routingRule = new RoundRobinClusterRoutingRule();
                    break;
                }
            }
            return routingRule;
        }).collect(Collectors.toList());
    }

    public static void update(RoutingRuleModel model, RoutingRuleSpec info)
            throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonString = objectMapper.writeValueAsString(info.getProperties());
        model.set(name, info.getName())
                .set(type, info.getName())
                .set(properties, jsonString);
        model.saveIt();
    }
}
