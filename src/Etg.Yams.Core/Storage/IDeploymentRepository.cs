using System.Threading.Tasks;
using Etg.Yams.Application;
using Etg.Yams.Storage.Config;
using Etg.Yams.Storage.Status;
using System;

namespace Etg.Yams.Storage
{
    public interface IDeploymentRepository : IDeploymentMonitor, IDeploymentStatusManager
    {
        Task<DeploymentConfig> FetchDeploymentConfig();
        Task PublishDeploymentConfig(DeploymentConfig deploymentConfig);
        Task UploadApplicationBinaries(AppIdentity appIdentity, string localPath, ConflictResolutionMode conflictResolutionMode);
        Task DeleteApplicationBinaries(AppIdentity appIdentity);
        Task<bool> HasApplicationBinaries(AppIdentity appIdentity);
        Task DownloadApplicationBinaries(AppIdentity appIdentity, string localPath, ConflictResolutionMode conflictResolutionMode);
    }

    public interface IDeploymentMonitor
    {
        Task<DeploymentStatus> FetchDeploymentStatus();
    }

    public interface IDeploymentStatusManager
    {
        Task PublishDeploymentStatus(DeploymentStatus deploymentStatus);

        /// <summary>
        /// Will atomically update the DeploymentStatus to avoid race conditions.
        /// The existing DeploymentStatus will be passed to the given action which is expected
        /// to update it.
        /// </summary>
        Task UpdateDeploymentStatusAtomically(Action<DeploymentStatus> updateAction);
    }
}