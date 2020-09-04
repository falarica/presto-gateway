package io.prestosql.gateway.persistence.dao;

import io.prestosql.gateway.persistence.ClusterDetail;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.IdName;
import org.javalite.activejdbc.annotations.Table;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Table("clusters")
@IdName("name")
public class Cluster
        extends Model
{
    private static final String name = "name";
    private static final String clusterUrl = "coordinator_url";
    private static final String adminName = "admin_name";
    private static final String adminPassword = "admin_password";
    private static final String location = "location";
    private static final String active = "active";

    public static List<ClusterDetail> upcast(List<Cluster> clusters)
    {
        return clusters.stream().map(cluster -> new ClusterDetail(cluster.getString("name"),
                cluster.getString(location),
                cluster.getString(clusterUrl),
                Optional.ofNullable(cluster.getString(adminName)),
                Optional.empty(),
                Optional.ofNullable(cluster.getString(adminPassword)),
                cluster.getBoolean(active), Optional.empty())).collect(Collectors.toList());
    }

    public static Cluster create(ClusterDetail info)
    {
        Cluster cluster = new Cluster();
        cluster = cluster.create(name, info.getName(),
                clusterUrl, info.getClusterUrl(),
                adminName, info.getAdminName().orElse(null),
                adminPassword, info.getAdminPassword().orElse(null),
                location, info.getLocation(),
                active, info.isActive());
        cluster.insert();

        return cluster;
    }

    public static void update(Cluster cluster, ClusterDetail info)
    {
        cluster.set(name, info.getName())
                .set(clusterUrl, info.getClusterUrl())
                .set(adminName, info.getAdminName().orElse(null))
                .set(adminPassword, info.getAdminPassword().orElse(null))
                .set(location, info.getLocation())
                .set(active, info.isActive())
                .saveIt();
    }
}
