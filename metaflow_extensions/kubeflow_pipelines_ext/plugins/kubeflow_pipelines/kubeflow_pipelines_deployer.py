from typing import Any, ClassVar, Dict, Optional, TYPE_CHECKING, Type

from metaflow.runner.deployer_impl import DeployerImpl

if TYPE_CHECKING:
    import metaflow_extensions.kubeflow_pipelines_ext.plugins.kubeflow_pipelines.kubeflow_pipelines_deployer_objects

class KubeflowPipelinesDeployer(DeployerImpl):
    """
    Deployer implementation for Kubeflow Pipelines.

    Parameters
    ----------
    name : str, optional, default None
        Kubeflow pipeline name. The flow name is used instead if this option is not specified.
    """

    TYPE: ClassVar[Optional[str]] = "kubeflow-pipelines"

    def __init__(self, deployer_kwargs: Dict[str, str], **kwargs):
        """
        Initialize the KubeflowPipelinesDeployer.

        Parameters
        ----------
        deployer_kwargs : Dict[str, str]
            The deployer-specific keyword arguments.
        **kwargs : Any
            Additional arguments to pass to the superclass constructor.
        """
        self._deployer_kwargs = deployer_kwargs
        super().__init__(**kwargs)

    @property
    def deployer_kwargs(self) -> Dict[str, Any]:
        return self._deployer_kwargs

    @staticmethod
    def deployed_flow_type() -> (
        Type[
            "metaflow_extensions.kubeflow_pipelines_ext.plugins.kubeflow_pipelines.kubeflow_pipelines_deployer_objects.KubeflowPipelinesDeployedFlow"
        ]
    ):
        from .kubeflow_pipelines_deployer_objects import KubeflowPipelinesDeployedFlow

        return KubeflowPipelinesDeployedFlow

    def create(
        self, **kwargs
    ) -> "metaflow_extensions.kubeflow_pipelines_ext.plugins.kubeflow_pipelines.kubeflow_pipelines_deployer_objects.KubeflowPipelinesDeployedFlow":
        """
        Create a new KubeflowPipelines deployment.

        Parameters
        ----------
        authorize : str, optional, default None
            Authorize using this production token. Required when re-deploying an existing flow
            for the first time. The token is cached in METAFLOW_HOME.
        generate_new_token : bool, default False
            Generate a new production token for this flow. This will move the production flow to a new namespace.
        given_token : str, optional, default None
            Use the given production token for this flow. This will move the production flow to the given namespace.
        tags : List[str], optional, default None
            Annotate all objects produced by Kubeflow Pipeline executions with these tags.
        user_namespace : str, optional, default None
            Change the namespace from the default to the given namespace.
        url : str, optional, default None
            The URL of the Kubeflow Pipelines API.
        version_name : str, optional, default None
            The version name of the pipeline to upload.
        experiment : str, optional, default None
            The experiment name to create the schedule under (if @schedule is present).
        only_yaml : bool, optional, default False
            Compile the pipeline to a YAML string and exit without uploading to Kubeflow Pipelines.
        max_workers : int, optional, default 100
            Maximum number of parallel processes.
        deployer_attribute_file : str, optional, default None
            Write the workflow name to the specified file. Used internally for Metaflow's Deployer API.

        Returns
        -------
        KubeflowPipelinesDeployedFlow
            The Flow deployed to Kubeflow Pipelines.
        """
        from .kubeflow_pipelines_deployer_objects import KubeflowPipelinesDeployedFlow

        return self._create(KubeflowPipelinesDeployedFlow, **kwargs)
