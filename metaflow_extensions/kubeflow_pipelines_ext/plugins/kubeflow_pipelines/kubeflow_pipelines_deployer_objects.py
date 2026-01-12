import sys
from typing import ClassVar, Optional
from metaflow.metaflow_config import KUBEFLOW_PIPELINES_URL

from metaflow.runner.deployer import DeployedFlow, TriggeredRun
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo

from .kubeflow_pipelines import KubeflowPipelines


class KubeflowPipelinesTriggeredRun(TriggeredRun):
    """
    A class representing a triggered Kubeflow Pipeline execution.
    """

    def terminate(self, **kwargs) -> bool:
        """
        Terminate the running workflow.

        Parameters
        ----------
        authorize : str, optional, default None
            Authorize the termination with a production token.

        Returns
        -------
        bool
            True if the command was successful, False otherwise.
        """
        _, run_id = self.pathspec.split("/")
        kfp_run_id = run_id[4:]

        # every subclass needs to have `self.deployer_kwargs`
        command = get_lower_level_group(
            self.deployer.api,
            self.deployer.top_level_kwargs,
            self.deployer.TYPE,
            self.deployer.deployer_kwargs,
        ).terminate(kfp_run_id=kfp_run_id, **kwargs)

        pid = self.deployer.spm.run_command(
            [sys.executable, *command],
            env=self.deployer.env_vars,
            cwd=self.deployer.cwd,
            show_output=self.deployer.show_output,
        )

        command_obj = self.deployer.spm.get(pid)
        command_obj.sync_wait()
        return command_obj.process.returncode == 0

    @property
    def status(self) -> Optional[str]:
        """
        Get the status of the triggered run.

        Returns
        -------
        str, optional
            The status of the workflow, or None if
            the status could not be retrieved.
        """

        from kfp import Client
        kfp_url = self.deployer.deployer_kwargs.get("url") or KUBEFLOW_PIPELINES_URL
        kfp_client = Client(host=kfp_url)

        _, run_id = self.pathspec.split("/")
        kfp_run_id = run_id[4:]

        return KubeflowPipelines.get_status(kfp_client, kfp_run_id)


class KubeflowPipelinesDeployedFlow(DeployedFlow):
    """
    A class representing a deployed Kubeflow Pipeline.
    """

    TYPE: ClassVar[Optional[str]] = "kubeflow-pipelines"

    @classmethod
    def list_deployed_flows(cls, flow_name: Optional[str] = None):
        """
        This method is not currently implemented for Kubeflow Pipelines.

        Raises
        ------
        NotImplementedError
            This method is not implemented for Kubeflow Pipelines.
        """
        raise NotImplementedError(
            "list_deployed_flows is not implemented for KubeflowPipelines"
        )

    @classmethod
    def from_deployment(cls, identifier: str, metadata: Optional[str] = None):
        """
        This method is not currently implemented for Kubeflow Pipelines.

        Raises
        ------
        NotImplementedError
            This method is not implemented for Kubeflow Pipelines.
        """
        raise NotImplementedError(
            "from_deployment is not implemented for KubeflowPipelines"
        )

    @classmethod
    def get_triggered_run(
        cls, identifier: str, run_id: str, metadata: Optional[str] = None
    ):
        """
        This method is not currently implemented for Kubeflow Pipelines.

        Raises
        ------
        NotImplementedError
            This method is not implemented for Kubeflow Pipelines.
        """
        raise NotImplementedError(
            "get_triggered_run is not implemented for KubeflowPipelines"
        )

    @property
    def production_token(self) -> Optional[str]:
        """
        Get the production token for the deployed flow.

        Returns
        -------
        str, optional
            The production token, None if it cannot be retrieved.
        """
        try:
            from kfp import Client
            kfp_url = self.deployer.deployer_kwargs.get("url") or KUBEFLOW_PIPELINES_URL
            kfp_client = Client(host=kfp_url)

            _, production_token = KubeflowPipelines.get_existing_deployment(
                kfp_client,
                self.deployer.name
            )
            return production_token
        except TypeError:
            return None

    def delete(self, **kwargs) -> bool:
        """
        Delete the deployed kubeflow pipeline.

        Parameters
        ----------
        authorize : str, optional, default None
            Authorize the deletion with a production token.

        Returns
        -------
        bool
            True if the command was successful, False otherwise.
        """
        command = get_lower_level_group(
            self.deployer.api,
            self.deployer.top_level_kwargs,
            self.deployer.TYPE,
            self.deployer.deployer_kwargs,
        ).delete(**kwargs)

        pid = self.deployer.spm.run_command(
            [sys.executable, *command],
            env=self.deployer.env_vars,
            cwd=self.deployer.cwd,
            show_output=self.deployer.show_output,
        )

        command_obj = self.deployer.spm.get(pid)
        command_obj.sync_wait()
        return command_obj.process.returncode == 0

    def trigger(self, **kwargs) -> KubeflowPipelinesTriggeredRun:
        """
        Trigger a new run for the deployed pipeline.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments to pass to the trigger command,
            `Parameters` in particular.

        Returns
        -------
        KubeflowPipelinesTriggeredRun
            The triggered run instance.

        Raises
        ------
        Exception
            If there is an error during the trigger process.
        """
        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            # every subclass needs to have `self.deployer_kwargs`
            command = get_lower_level_group(
                self.deployer.api,
                self.deployer.top_level_kwargs,
                self.deployer.TYPE,
                self.deployer.deployer_kwargs,
            ).trigger(deployer_attribute_file=attribute_file_path, **kwargs)

            pid = self.deployer.spm.run_command(
                [sys.executable, *command],
                env=self.deployer.env_vars,
                cwd=self.deployer.cwd,
                show_output=self.deployer.show_output,
            )

            command_obj = self.deployer.spm.get(pid)
            content = handle_timeout(
                attribute_file_fd, command_obj, self.deployer.file_read_timeout
            )
            command_obj.sync_wait()
            if command_obj.process.returncode == 0:
                return KubeflowPipelinesTriggeredRun(
                    deployer=self.deployer, content=content
                )

        raise Exception(
            "Error triggering %s on %s for %s"
            % (
                self.deployer.name,
                self.deployer.TYPE,
                self.deployer.flow_file,
            )
        )
