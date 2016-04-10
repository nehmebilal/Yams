using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Etg.Yams.Application;
using Etg.Yams.Azure.Utils;
using Etg.Yams.Storage;
using Etg.Yams.Storage.Config;
using Etg.Yams.Utils;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Etg.Yams.Azure.Storage
{
    public class BlobStorageDeploymentRepository : IDeploymentRepository
    {
        public const string ApplicationsRootFolderName = "applications";
        private readonly CloudBlobContainer _blobContainer;

        public BlobStorageDeploymentRepository(CloudBlobContainer blobContainer)
        {
            _blobContainer = blobContainer;
        }

        public BlobStorageDeploymentRepository(string connectionString) : this(GetApplicationsContainerReference(connectionString))
        {
        }

        private static CloudBlobContainer GetApplicationsContainerReference(string connectionString)
        {
            CloudBlobContainer blobContainer = BlobUtils.GetBlobContainer(connectionString,
                ApplicationsRootFolderName);
            return blobContainer;
        }

        public async Task DeleteApplicationBinaries(AppIdentity appIdentity)
        {
            CloudBlobDirectory blobDirectory = GetBlobDirectory(appIdentity);
            if (!await blobDirectory.ExistsAsync())
            {
                throw new BinariesNotFoundException(
                    $"Cannot delete binaries for application {appIdentity} because they were not found");
            }
            await blobDirectory.DeleteAsync();
        }

        public async Task<DeploymentConfig> FetchDeploymentConfig()
        {
            var blob = _blobContainer.GetBlockBlobReference(Constants.DeploymentConfigFileName);
            if (!await blob.ExistsAsync())
            {
                Trace.TraceInformation("The DeploymentConfig.json file was not found in the Yams repository");
                return new DeploymentConfig();
            }

            string leaseId = await LockDeploymentConfig(blob);
            try
            {
                string data = await blob.DownloadTextAsync();
                return new DeploymentConfig(data);
            }
            finally
            {
                await ReleaseLock(blob, leaseId);
            }
        }

        public Task<bool> HasApplicationBinaries(AppIdentity appIdentity)
        {
            return GetBlobDirectory(appIdentity).ExistsAsync();
        }

        public async Task DownloadApplicationBinaries(AppIdentity appIdentity, string localPath,
            ConflictResolutionMode conflictResolutionMode)
        {
            bool exists = !FileUtils.DirectoryDoesntExistOrEmpty(localPath);
            if (exists)
            {
                if (conflictResolutionMode == ConflictResolutionMode.DoNothingIfBinariesExist)
                {
                    return;
                }
                if (conflictResolutionMode == ConflictResolutionMode.FailIfBinariesExist)
                {
                    throw new DuplicateBinariesException(
                        $"Cannot download the binaries because the destination directory {localPath} contains files");
                }
            }
            CloudBlobDirectory blobDirectory = GetBlobDirectory(appIdentity);
            if (!await blobDirectory.ExistsAsync())
            {
                throw new BinariesNotFoundException("The binaries were not found in the Yams repository");
            }
            await BlobUtils.DownloadBlobDirectory(blobDirectory, localPath);
        }

        public async Task PublishDeploymentConfig(DeploymentConfig deploymentConfig)
        {
            CloudBlockBlob blob = _blobContainer.GetBlockBlobReference(Constants.DeploymentConfigFileName);
            string leaseId = await LockDeploymentConfig(blob);
            try
            {
                await blob.UploadTextAsync(deploymentConfig.RawData());
            }
            finally
            {
                await ReleaseLock(blob, leaseId);
            }
        }

        private static async Task ReleaseLock(CloudBlockBlob blob, string leaseId)
        {
            if (leaseId == null)
            {
                return;
            }
            await blob.ReleaseLeaseAsync(
                new AccessCondition
                {
                    LeaseId = leaseId
                });
        }

        private static async Task<string> LockDeploymentConfig(CloudBlockBlob blob)
        {
            // There is a slim chance of race condition if the DeploymentConfig.json doesn't exist.
            // The problem is that we cannot lock the blob if it doesn't exist.
            if (!await blob.ExistsAsync())
            {
                return null;
            }
            string leaseId = await blob.AcquireLeaseAsync(null, null);
            if (leaseId == null)
            {
                throw new DeploymentConfigLockedException();
            }

            return leaseId;
        }

        public async Task UploadApplicationBinaries(AppIdentity appIdentity, string localPath,
            ConflictResolutionMode conflictResolutionMode)
        {
            if (FileUtils.DirectoryDoesntExistOrEmpty(localPath))
            {
                throw new BinariesNotFoundException(
                    $"Binaries were not be uploaded because they were not found at the given path {localPath}");
            }

            if (conflictResolutionMode == ConflictResolutionMode.OverwriteExistingBinaries)
            {
                await DeleteApplicationBinaries(appIdentity);
            }
            else
            {
                bool exists = await HasApplicationBinaries(appIdentity);

                if (exists)
                {
                    if (conflictResolutionMode == ConflictResolutionMode.DoNothingIfBinariesExist)
                    {
                        return;
                    }

                    if (conflictResolutionMode == ConflictResolutionMode.FailIfBinariesExist)
                    {
                        throw new DuplicateBinariesException(
                            $"Cannot override binaries when flag {ConflictResolutionMode.FailIfBinariesExist} is used");
                    }
                }
            }

            // at this point we know that it is either OverwriteExistingBinaries mode or the binaries don't exist
            await BlobUtils.UploadDirectory(localPath, _blobContainer, GetBlobDirectoryRelPath(appIdentity));
        }

        private CloudBlobDirectory GetBlobDirectory(AppIdentity appIdentity)
        {
            string relPath = GetBlobDirectoryRelPath(appIdentity);
            return _blobContainer.GetDirectoryReference(relPath);
        }

        private string GetBlobDirectoryRelPath(AppIdentity appIdentity)
        {
            return appIdentity.Id + "/" + appIdentity.Version;
        }
    }
}