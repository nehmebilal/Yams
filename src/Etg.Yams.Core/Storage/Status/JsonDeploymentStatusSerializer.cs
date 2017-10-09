using Etg.Yams.Json;
using System.Collections.Generic;

namespace Etg.Yams.Storage.Status
{
    public class JsonDeploymentStatusSerializer : IDeploymentStatusSerializer
    {
        private readonly IJsonSerializer _jsonSerializer;

        public JsonDeploymentStatusSerializer(IJsonSerializer jsonSerializer)
        {
            _jsonSerializer = jsonSerializer;
        }

        public DeploymentStatus Deserialize(string data)
        {
            var apps = _jsonSerializer.Deserialize<IEnumerable<AppDeploymentStatus>>(data);
            return new DeploymentStatus(apps);
        }

        public string Serialize(DeploymentStatus deploymentStatus)
        {
            return _jsonSerializer.Serialize(deploymentStatus.ListAll());
        }
    }
}
