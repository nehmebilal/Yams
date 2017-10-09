using Etg.Yams.Application;
using Newtonsoft.Json;
using Semver;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Etg.Yams.Storage.Status
{
    public class AppDeploymentStatus
    {
        public AppDeploymentStatus(AppIdentity appIdentity, string clusterId, string instanceId, 
            DateTime utcTimeStamp)
        {
            AppIdentity = appIdentity;
            ClusterId = clusterId;
            InstanceId = instanceId;
            UtcTimeStamp = utcTimeStamp;
        }

        [JsonConstructor]
        public AppDeploymentStatus(string id, string version, string clusterId, string instanceId, 
            DateTime utcTimeStamp) : this(new AppIdentity(id, version), clusterId, instanceId, utcTimeStamp)
        {
        }

        [JsonIgnore]
        public AppIdentity AppIdentity { get; private set; }

        public string Id => AppIdentity.Id;
        public string Version => AppIdentity.Version.ToString();
        public string ClusterId { get; }
        public string InstanceId { get; private set; }
        public DateTime UtcTimeStamp { get; }

        private AppDeploymentStatus Clone()
        {
            return new AppDeploymentStatus(AppIdentity, ClusterId, InstanceId, DateTime.UtcNow);
        }

        private AppDeploymentStatus WithVersion(SemVersion version)
        {
            var appDeploymentStatus = Clone();
            appDeploymentStatus.AppIdentity = new AppIdentity(appDeploymentStatus.AppIdentity.Id, version);
            return appDeploymentStatus;
        }
    }

    public class DeploymentStatus
    {
        private Dictionary<string, ClusterDeploymentStatus> _clusters = new Dictionary<string, ClusterDeploymentStatus>();

        public DeploymentStatus()
        {
        }
        public DeploymentStatus(IEnumerable<AppDeploymentStatus> apps)
        {
            foreach(AppDeploymentStatus appDeploymentStatus in apps)
            {
                SetAppDeploymentStatus(appDeploymentStatus);
            }
        }

        public void SetClusterDeploymentStatus(string clusterId, ClusterDeploymentStatus clusterDeploymentStatus)
        {
            _clusters[clusterId] = clusterDeploymentStatus;
        }

        public AppDeploymentStatus GetAppDeploymentStatus(string clusterId, string instanceId, AppIdentity appIdentity)
        {
            ClusterDeploymentStatus clusterDeploymentStatus;
            if (!_clusters.TryGetValue(clusterId, out clusterDeploymentStatus))
            {
                return null;
            }
            return clusterDeploymentStatus.GetAppDeploymentStatus(instanceId, appIdentity);
        }

        public void SetAppDeploymentStatus(AppDeploymentStatus appDeploymentStatus)
        {
            ClusterDeploymentStatus clusterDeploymentStatus;
            if (!_clusters.TryGetValue(appDeploymentStatus.ClusterId, out clusterDeploymentStatus))
            {
                clusterDeploymentStatus = new ClusterDeploymentStatus();
                _clusters[appDeploymentStatus.ClusterId] = clusterDeploymentStatus;
            }
            clusterDeploymentStatus.SetAppDeploymentStatus(appDeploymentStatus);
        }

        public ClusterDeploymentStatus GetClusterDeploymentStatus(string clusterId)
        {
            ClusterDeploymentStatus clusterDeploymentStatus;
            if (!_clusters.TryGetValue(clusterId, out clusterDeploymentStatus))
            {
                return null;
            }
            return clusterDeploymentStatus;
        }

        public IEnumerable<AppDeploymentStatus> ListAll()
        {
            return _clusters.Values.SelectMany(cluster => cluster.ListAll());
        }
    }

    public class ClusterDeploymentStatus
    {
        Dictionary<string, InstanceDeploymentsStatus> _instances = new Dictionary<string, InstanceDeploymentsStatus>();

        public AppDeploymentStatus GetAppDeploymentStatus(string instanceId, AppIdentity appIdentity)
        {
            InstanceDeploymentsStatus instanceDeploymentsStatus;
            if (!_instances.TryGetValue(instanceId, out instanceDeploymentsStatus))
            {
                return null;
            }
            return instanceDeploymentsStatus.GetAppDeploymentStatus(appIdentity);
        }

        public void SetAppDeploymentStatus(AppDeploymentStatus appDeploymentStatus)
        {
            InstanceDeploymentsStatus instanceDeploymentsStatus;
            if (!_instances.TryGetValue(appDeploymentStatus.InstanceId, out instanceDeploymentsStatus))
            {
                instanceDeploymentsStatus = new InstanceDeploymentsStatus();
                _instances[appDeploymentStatus.InstanceId] = instanceDeploymentsStatus;
            }
            instanceDeploymentsStatus.SetAppDeploymentStatus(appDeploymentStatus);
        }

        public void SetInstanceDeploymentStatus(string instanceId, InstanceDeploymentsStatus status)
        {
            _instances[instanceId] = status;
        }

        public IEnumerable<AppDeploymentStatus> ListAll()
        {
            return _instances.Values.SelectMany(instance => instance.ListAll());
        }
    }

    public class InstanceDeploymentsStatus
    {
        private Dictionary<AppIdentity, AppDeploymentStatus> _apps = new Dictionary<AppIdentity, AppDeploymentStatus>();

        public AppDeploymentStatus GetAppDeploymentStatus(AppIdentity appIdentity)
        {
            AppDeploymentStatus appDeploymentStatus;
            if (!_apps.TryGetValue(appIdentity, out appDeploymentStatus))
            {
                return null;
            }
            return appDeploymentStatus;
        }

        public void SetAppDeploymentStatus(AppDeploymentStatus appDeploymentStatus)
        {
            _apps[appDeploymentStatus.AppIdentity] = appDeploymentStatus;
        }

        public IEnumerable<AppDeploymentStatus> ListAll()
        {
            return _apps.Values;
        }
    }

    public interface IDeploymentStatusSerializer
    {
        DeploymentStatus Deserialize(string data);
        string Serialize(DeploymentStatus deploymentStatus);
    }
}
