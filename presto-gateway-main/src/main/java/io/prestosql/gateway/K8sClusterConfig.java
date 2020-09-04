package io.prestosql.gateway;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class K8sClusterConfig
{
    private String gcloud = "/usr/bin/gcloud";
    private String kubectl = "/usr/local/bin/kubectl";
    private String helm = "/usr/local/bin/helm";
    private String chartBaseDir = "prestochart";
    private String tmpDir = "etc/tmpDir/";

    public String getGcloudPath()
    {
        return gcloud;
    }

    @Config("k8scluster.gcloud")
    @ConfigDescription("Path of the gcloud binary")
    public K8sClusterConfig setGcloudPath(String path)
    {
        this.gcloud = path;
        return this;
    }

    public String getKubectlPath()
    {
        return kubectl;
    }

    @Config("k8scluster.kubectl")
    @ConfigDescription("Path of the kubectl binary")
    public K8sClusterConfig setKubectlPath(String path)
    {
        this.kubectl = path;
        return this;
    }

    public String getHelmPath()
    {
        return helm;
    }

    @Config("k8scluster.helm")
    @ConfigDescription("Path of the helm binary")
    public K8sClusterConfig setHelmPath(String path)
    {
        this.helm = path;
        return this;
    }

    public String getChartBaseDirPath()
    {
        return chartBaseDir;
    }

    @Config("k8scluster.chartdir")
    @ConfigDescription("Path of presto charts")
    public K8sClusterConfig setChartBaseDirPath(String path)
    {
        this.chartBaseDir = path;
        return this;
    }

    public String getTmpDirPath()
    {
        return tmpDir;
    }

    @Config("k8scluster.tmpsdirpath")
    @ConfigDescription("Path of tmp service directory to store files")
    public K8sClusterConfig setTmpDirPath(String path)
    {
        this.tmpDir = path;
        return this;
    }
}
