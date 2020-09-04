package io.prestosql.gateway.persistence;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class ClusterDetail
{
    private final String location;
    private final String name;
    private final String clusterUrl;
    private final Optional<String> adminName;
    private final Optional<String> adminPassword;
    private final boolean active;
    private final Optional<Duration> uptime;

    @JsonCreator
    public ClusterDetail(
            @JsonProperty("name") String name,
            @JsonProperty("location") String location,
            @JsonProperty("clusterUrl") String clusterUrl,
            @JsonProperty("adminName") Optional<String> adminName,
            @JsonProperty("adminPassword") Optional<String> adminPassword,
            @JsonProperty("adminPasswordEncoded") Optional<String> adminPasswordEncoded,
            @JsonProperty("active") boolean active,
            @JsonProperty("uptime") Optional<Duration> uptime)
    {
        this.name = requireNonNull(name, "name is null");
        this.location = requireNonNull(location, "location is null");
        validateURI(clusterUrl);
        this.clusterUrl = requireNonNull(clusterUrl, "cluster url is null");
        this.adminName = requireNonNull(adminName, "adminName is null");
        Base64.Encoder encoder = Base64.getEncoder();
        Optional<String> password = requireNonNull(adminPassword, "adminpassword is null");
        if (password.isPresent()) {
            this.adminPassword = Optional.of(new String(encoder.encode(password.get()
                    .getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));
        }
        else {
            if (adminPasswordEncoded.isPresent()) {
                this.adminPassword = adminPasswordEncoded;
            }
            else {
                this.adminPassword = Optional.empty();
            }
        }
        this.active = active;
        // this.uptime = requireNonNull(uptime, "uptime is null");
        this.uptime = uptime;
    }

    private void validateURI(String urlString)
    {
        URI.create(urlString);
    }

    @JsonProperty
    public Optional<String> getAdminName()
    {
        return adminName;
    }

    @JsonProperty
    public Optional<String> getAdminPassword()
    {
        return this.adminPassword;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public String getClusterUrl()
    {
        return clusterUrl;
    }

    @JsonProperty
    public boolean isActive()
    {
        return active;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Optional<Duration> getUptime()
    {
        return uptime;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClusterDetail that = (ClusterDetail) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(clusterUrl, that.clusterUrl) &&
                Objects.equals(adminName, that.adminName) &&
                Objects.equals(adminPassword, that.adminPassword);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, clusterUrl);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("location", location)
                .add("coordinator_url", clusterUrl)
                .add("active", active)
                //.add("uptime", uptime.orElse(null))
                .omitNullValues()
                .toString();
    }
}
