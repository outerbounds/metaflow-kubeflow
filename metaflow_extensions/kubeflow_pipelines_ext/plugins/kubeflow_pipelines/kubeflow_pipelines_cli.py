import re
import json
import base64
import tempfile
from hashlib import sha1

from metaflow._vendor import click
from metaflow import current, decorators, parameters
from metaflow.package import MetaflowPackage
from metaflow.util import get_username, to_bytes, to_unicode
from metaflow.metaflow_config import FEAT_ALWAYS_UPLOAD_CODE_PACKAGE, KUBEFLOW_PIPELINES_URL
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.plugins.aws.step_functions.production_token import (
    load_token,
    new_token,
    store_token,
)
from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator

from .kubeflow_pipelines import KubeflowPipelines
from .kubeflow_pipelines_exceptions import KubeflowPipelineException


class RunIdMismatch(MetaflowException):
    headline = "Run ID mismatch"


class IncorrectProductionToken(MetaflowException):
    headline = "Incorrect production token"


VALID_NAME = re.compile(r"[^a-zA-Z0-9_\-\.]")


def resolve_token(
    kfp_client,
    name,
    token_prefix,
    obj,
    authorize,
    given_token,
    generate_new_token,
    is_project
):
    # 1) retrieve the previous deployment, if one exists
    workflow = KubeflowPipelines.get_existing_deployment(kfp_client, name)
    if workflow is None:
        obj.echo(
            "It seems this is the first time you are deploying *%s* to "
            "Kubeflow Pipelines." % name
        )
        prev_token = None
    else:
        prev_user, prev_token = workflow

    # 2) authorize this deployment
    if prev_token is not None:
        if authorize is None:
            authorize = load_token(token_prefix)
        elif authorize.startswith("production:"):
            authorize = authorize[11:]

        # we allow the user who deployed the previous version to re-deploy,
        # even if they don't have the token
        if prev_user != get_username() and authorize != prev_token:
            obj.echo(
                "There is an existing version of *%s* on Kubeflow Pipelines which was "
                "deployed by the user *%s*." % (name, prev_user)
            )
            obj.echo(
                "To deploy a new version of this flow, you need to use the same "
                "production token that they used. "
            )
            obj.echo(
                "Please reach out to them to get the token. Once you have it, call "
                "this command:"
            )
            obj.echo("    kubeflow-pipelines create --authorize MY_TOKEN", fg="green")
            obj.echo(
                'See "Organizing Results" at docs.metaflow.org for more information '
                "about production tokens."
            )
            raise IncorrectProductionToken(
                "Try again with the correct production token."
            )

    # 3) do we need a new token or should we use the existing token?
    if given_token:
        if is_project:
            # we rely on a known prefix for @project tokens, so we can't
            # allow the user to specify a custom token with an arbitrary prefix
            raise MetaflowException(
                "--new-token is not supported for @projects. Use --generate-new-token "
                "to create a new token."
            )
        if given_token.startswith("production:"):
            given_token = given_token[11:]
        token = given_token
        obj.echo("")
        obj.echo("Using the given token, *%s*." % token)
    elif prev_token is None or generate_new_token:
        token = new_token(token_prefix, prev_token)
        if token is None:
            if prev_token is None:
                raise MetaflowInternalError(
                    "We could not generate a new token. This is unexpected. "
                )
            else:
                raise MetaflowException(
                    "--generate-new-token option is not supported after using "
                    "--new-token. Use --new-token to make a new namespace."
                )
        obj.echo("")
        obj.echo("A new production token generated.")
    else:
        token = prev_token

    obj.echo("")
    obj.echo("The namespace of this production flow is")
    obj.echo("    production:%s" % token, fg="green")
    obj.echo(
        "To analyze results of this production flow add this line in your notebooks:"
    )
    obj.echo('    namespace("production:%s")' % token, fg="green")
    obj.echo(
        "If you want to authorize other people to deploy new versions of this flow to "
        "Kubeflow Pipelines, they need to call"
    )
    obj.echo("    kubeflow-pipelines create --authorize %s" % token, fg="green")
    obj.echo("when deploying this flow to Kubeflow Pipelines for the first time.")
    obj.echo(
        'See "Organizing Results" at https://docs.metaflow.org/ for more '
        "information about production tokens."
    )
    obj.echo("")
    store_token(token_prefix, token)

    return token


def resolve_pipeline_name(name):
    project = current.get("project_name")
    is_project = False

    if project:
        is_project = True
        if name:
            raise MetaflowException(
                "--name is not supported for @projects. " "Use --branch instead."
            )
        pipeline_name = current.project_flow_name
        if pipeline_name and VALID_NAME.search(pipeline_name):
            raise MetaflowException(
                "Name '%s' contains invalid characters. Please construct a name using regex %s"
                % (pipeline_name, VALID_NAME.pattern)
            )
        project_branch = to_bytes(".".join((project, current.branch_name)))
        token_prefix = (
            "mfprj-%s"
            % to_unicode(base64.b32encode(sha1(project_branch).digest()))[:16]
        )
    else:
        if name and VALID_NAME.search(name):
            raise MetaflowException(
                "Name '%s' contains invalid characters. Please construct a name using regex %s"
                % (name, VALID_NAME.pattern)
            )
        pipeline_name = name if name else current.flow_name
        token_prefix = pipeline_name
    return pipeline_name, token_prefix.lower(), is_project


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Kubeflow Pipelines.")
@click.option(
    "--name",
    default=None,
    type=str,
    help="Kubeflow Pipeline name. The flow name is used instead if this option is not "
    "specified",
)
@click.pass_obj
def kubeflow_pipelines(obj, name=None):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    obj.pipeline_name, obj.token_prefix, obj.is_project = resolve_pipeline_name(name)


@parameters.add_custom_parameters(deploy_mode=True)
@kubeflow_pipelines.command(help="Compile a new version of this flow to Kubeflow Pipeline.")
@click.option(
    "--authorize",
    default=None,
    help="Authorize using this production token. You need this "
    "when you are re-deploying an existing flow for the first "
    "time. The token is cached in METAFLOW_HOME, so you only "
    "need to specify this once.",
)
@click.option(
    "--generate-new-token",
    is_flag=True,
    help="Generate a new production token for this flow. "
    "This will move the production flow to a new namespace.",
)
@click.option(
    "--new-token",
    "given_token",
    default=None,
    help="Use the given production token for this flow. "
    "This will move the production flow to the given namespace.",
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
    help="Annotate all objects produced by Kubeflow Pipeline executions "
    "with the given tag. You can specify this option multiple "
    "times to attach multiple tags.",
)
@click.option(
    "--namespace",
    "user_namespace",
    default=None,
    help="Change the namespace from the default to the given namespace. "
    "See run --help for more information.",
)
@click.option(
    "--url",
    default=KUBEFLOW_PIPELINES_URL,
    show_default=True,
    help="The URL of the Kubeflow Pipelines API.",
)
@click.option(
    "--version-name",
    default=None,
    help="The version name of the pipeline to upload.",
)
@click.option(
    "--experiment",
    default=None,
    help="The experiment name to create the schedule under (if @schedule is present).",
)
@click.option(
    "--yaml-only",
    is_flag=True,
    default=False,
    help="Compile the pipeline to a local YAML file and exit without uploading to Kubeflow Pipelines.",
)
@click.option(
    "--max-workers",
    default=100,
    show_default=True,
    help="Maximum number of parallel processes.",
)
@click.pass_obj
def create(
    obj,
    authorize=None,
    generate_new_token=False,
    given_token=None,
    tags=None,
    user_namespace=None,
    url=None,
    version_name=None,
    experiment=None,
    yaml_only=False,
    max_workers=None,
    **kwargs,
):
    if not yaml_only and not url:
        raise KubeflowPipelineException("Please supply a Kubeflow Pipelines API Server URL with --url")

    from kfp import Client
    kfp_client = Client(host=url) if url else None

    obj.echo("Compiling *%s* to Kubeflow Pipelines..." % obj.pipeline_name, bold=True)
    token = resolve_token(
        kfp_client,
        obj.pipeline_name,
        obj.token_prefix,
        obj,
        authorize,
        given_token,
        generate_new_token,
        obj.is_project,
    )

    flow = make_flow(
        obj,
        obj.pipeline_name,
        token,
        tags,
        user_namespace,
        max_workers,
        kfp_client,
    )

    params = {}
    for _, param in obj.flow._get_parameters():
        k = param.name.replace("-", "_").lower()
        val = kwargs.get(k)
        if val is not None:
            if param.kwargs.get("type") == parameters.JSONType:
                if not isinstance(val, str):
                    val = json.dumps(val)
            elif isinstance(val, parameters.DelayedEvaluationParameter):
                val = val(return_str=True)
            params[param.name] = val

    if yaml_only:
        pipeline_path = f"{obj.pipeline_name}.yaml"
        flow.compile(pipeline_path)
        obj.echo("Pipeline YAML saved to: *{pipeline_path}*".format(pipeline_path=pipeline_path), bold=True)
        return

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=True) as tmp:
        flow.compile(tmp.name)
        result = flow.upload(tmp.name, version_name)

        pipeline_url = "{base_url}/#/pipelines/details/{pipeline_id}/version/{version_id}".format(
            base_url=url.rstrip('/'),
            pipeline_id=result['pipeline_id'],
            version_id=result['version_id']
        )

        obj.echo(
            "Version *{version_name}* of pipeline *{pipeline_name}* "
            "for flow *{name}* compiled to "
            "Kubeflow Pipelines successfully.\n".format(
                version_name=result["version_name"],
                pipeline_name=obj.pipeline_name,
                name=current.flow_name,
            ),
            bold=True,
        )

        recurring_run_result = flow.schedule(
            parameters=params,
            experiment_name=experiment,
            version_name=result['version_name'],
        )

        obj.echo("View at *{pipeline_url}*".format(pipeline_url=pipeline_url), bold=True)

        if recurring_run_result:
            obj.echo(
                "\nRecurring run created successfully.\n"
                "Recurring Run ID: *{id}*\n"
                "Name: *{name}*".format(
                    id=recurring_run_result['recurring_run_id'],
                    name=recurring_run_result['recurring_run_name']
                ),
                bold=True
            )


@parameters.add_custom_parameters(deploy_mode=False)
@kubeflow_pipelines.command(help="Trigger the workflow on Kubeflow Pipelines.")
@click.option(
    "--url",
    default=KUBEFLOW_PIPELINES_URL,
    show_default=True,
    help="The URL of the Kubeflow Pipelines API.",
)
@click.option(
    "--experiment",
    default=None,
    help="The experiment name to trigger the run under.",
)
@click.option(
    "--version-name",
    default=None,
    help="The version name of the pipeline to trigger.",
)
@click.pass_obj
def trigger(obj, url=None, experiment=None, version_name=None, **kwargs):
    if not url:
        raise KubeflowPipelineException("Please supply a Kubeflow Pipelines API Server URL with --url")

    from kfp import Client
    kfp_client = Client(host=url)

    params = {}
    for _, param in obj.flow._get_parameters():
        k = param.name.replace("-", "_").lower()
        val = kwargs.get(k)
        if val is not None:
            if param.kwargs.get("type") == parameters.JSONType:
                if not isinstance(val, str):
                    val = json.dumps(val)
            elif isinstance(val, parameters.DelayedEvaluationParameter):
                val = val(return_str=True)
            params[param.name] = val

    obj.echo(
        "Triggering *%s* on Kubeflow Pipelines..." % obj.pipeline_name,
        bold=True,
    )

    result = KubeflowPipelines.trigger(
        kfp_client,
        obj.pipeline_name,
        params,
        experiment,
        version_name,
    )

    run_url = "{base_url}/#/runs/details/{run_id}".format(
        base_url=url.rstrip('/'),
        run_id=result['run_id']
    )

    obj.echo(
        "Pipeline *{pipeline_name}* triggered successfully.\n"
        "Version: *{version_name}*\n"
        "Run ID: *{run_id}*".format(
            pipeline_name=obj.pipeline_name,
            version_name=result['version_name'],
            run_id=result['run_id'],
        ),
        bold=True,
    )
    obj.echo("View Run at *{run_url}*".format(run_url=run_url), bold=True)


@kubeflow_pipelines.command(help="Fetch flow execution status on Kubeflow Pipelines.")
@click.option(
    "--url",
    default=KUBEFLOW_PIPELINES_URL,
    show_default=True,
    help="The URL of the Kubeflow Pipelines API.",
)
@click.option(
    "--kfp-run-id",
    required=True,
    default=None,
    type=str,
    help="Kubeflow Pipeline Run ID.",
)
@click.pass_obj
def status(obj, url=None, kfp_run_id=None):
    if not url:
        raise KubeflowPipelineException("Please supply a Kubeflow Pipelines API Server URL with --url")

    obj.echo(
        "Fetching status for run *{kfp_run_id}* ...".format(kfp_run_id=kfp_run_id),
        bold=True,
    )

    from kfp import Client
    kfp_client = Client(host=url)
    run_status = KubeflowPipelines.get_status(kfp_client, kfp_run_id)

    if run_status:
        obj.echo("Status: *{status}*".format(status=run_status))
    else:
        obj.echo("Run *{kfp_run_id}* not found.".format(kfp_run_id=kfp_run_id))


@kubeflow_pipelines.command(help="Terminate flow execution on Kubeflow Pipelines.")
@click.option(
    "--url",
    default=KUBEFLOW_PIPELINES_URL,
    show_default=True,
    help="The URL of the Kubeflow Pipelines API.",
)
@click.option(
    "--authorize",
    default=None,
    type=str,
    help="Authorize the termination with a production token",
)
@click.option(
    "--kfp-run-id",
    required=True,
    default=None,
    type=str,
    help="Kubeflow Pipeline Run ID.",
)
@click.pass_obj
def terminate(obj, url=None, authorize=None, kfp_run_id=None):
    if not url:
        raise KubeflowPipelineException("Please supply a Kubeflow Pipelines API Server URL with --url")

    def _token_instructions(flow_name, prev_user):
        obj.echo(
            "There is an existing version of *%s* on Kubeflow Pipelines which was "
            "deployed by the user *%s*." % (flow_name, prev_user)
        )
        obj.echo(
            "To terminate this flow, you need to use the same production token that they used."
        )
        obj.echo(
            "Please reach out to them to get the token. Once you have it, call "
            "this command:"
        )
        obj.echo("    kubeflow-pipelines terminate --authorize MY_TOKEN --kfp-run-id RUN_ID", fg="green")
        obj.echo(
            'See "Organizing Results" at docs.metaflow.org for more information '
            "about production tokens."
        )

    obj.echo(
        "Terminating run *{run_id}* for {flow_name} ...".format(
            run_id=kfp_run_id, flow_name=obj.flow.name
        ),
        bold=True,
    )

    from kfp import Client
    kfp_client = Client(host=url)

    validate_run_id(
        kfp_client,
        obj.pipeline_name,
        kfp_run_id,
        obj.token_prefix,
        authorize,
        _token_instructions,
    )

    terminate_status = KubeflowPipelines.terminate_run(kfp_client, kfp_run_id)

    if terminate_status:
        obj.echo("\nRun terminated.")
    else:
        obj.echo("\nRun has already finished.")


@kubeflow_pipelines.command(help="Delete the flow from Kubeflow Pipelines.")
@click.option(
    "--url",
    default=KUBEFLOW_PIPELINES_URL,
    show_default=True,
    help="The URL of the Kubeflow Pipelines API.",
)
@click.option(
    "--authorize",
    default=None,
    type=str,
    help="Authorize the termination with a production token",
)
@click.pass_obj
def delete(obj, url=None, authorize=None):
    if not url:
        raise KubeflowPipelineException("Please supply a Kubeflow Pipelines API Server URL with --url")

    def _token_instructions(flow_name, prev_user):
        obj.echo(
            "There is an existing version of *%s* on Kubeflow Pipelines which was "
            "deployed by the user *%s*." % (flow_name, prev_user)
        )
        obj.echo(
            "To delete this flow, you need to use the same production token that they used."
        )
        obj.echo(
            "Please reach out to them to get the token. Once you have it, call "
            "this command:"
        )
        obj.echo("    kubeflow-pipelines delete --authorize MY_TOKEN", fg="green")
        obj.echo(
            'See "Organizing Results" at docs.metaflow.org for more information '
            "about production tokens."
        )

    from kfp import Client
    kfp_client = Client(host=url)

    deployment = KubeflowPipelines.get_existing_deployment(kfp_client, obj.pipeline_name)

    if deployment is None:
        prev_token = None
    else:
        prev_user, prev_token = deployment

    if prev_token is not None:
        if authorize is None:
            authorize = load_token(obj.token_prefix)
        elif authorize.startswith("production:"):
            authorize = authorize[11:]

        if prev_user != get_username() and authorize != prev_token:
            _token_instructions(obj.pipeline_name, prev_user)
            raise IncorrectProductionToken(
                "Try again with the correct production token."
            )

    obj.echo("Deleting pipeline *{name}*...".format(name=obj.pipeline_name), bold=True)
    deleted = KubeflowPipelines.delete(kfp_client, obj.pipeline_name)

    if deleted:
        obj.echo(f"Pipeline *{obj.pipeline_name}* deleted.")


def validate_run_id(
    kfp_client,
    name,
    run_id,
    token_prefix,
    authorize,
    instructions_fn=None,
):
    from kfp.client.client import kfp_server_api


    try:
        run_detail = kfp_client.get_run(run_id)

        pipeline_spec = run_detail.pipeline_spec
        if pipeline_spec is None:
            pipeline_version_ref = run_detail.pipeline_version_reference
            pipeline_id = pipeline_version_ref.pipeline_id
            version_id = pipeline_version_ref.pipeline_version_id
            pipeline_version_obj = kfp_client.get_pipeline_version(
                pipeline_id,
                version_id,
            )
            pipeline_spec = pipeline_version_obj.pipeline_spec

        (
            owner,
            token,
            flow_name,
            project_name,
            branch_name
        ) = KubeflowPipelines.extract_metadata_from_kfp_spec(name, pipeline_spec)

        if current.flow_name != flow_name:
            raise RunIdMismatch(
                "The pipeline with the run_id *%s* belongs to the flow *%s*, not for the flow *%s*."
                % (run_id, flow_name, current.flow_name)
            )

        if project_name is not None:
            if current.get("project_name") != project_name:
                raise RunIdMismatch(
                    "The pipeline belongs to the project *%s*. "
                    "Please use the project decorator or --name to target the correct project"
                    % project_name
                )

            if current.get("branch_name") != branch_name:
                raise RunIdMismatch(
                    "The pipeline belongs to the branch *%s*. "
                    "Please use --branch, --production or --name to target the correct branch"
                    % branch_name
                )

        if authorize is None:
            authorize = load_token(token_prefix)
        elif authorize.startswith("production:"):
            authorize = authorize[11:]

        if owner != get_username() and authorize != token:
            if instructions_fn:
                instructions_fn(flow_name=name, prev_user=owner)
            raise IncorrectProductionToken("Try again with the correct production token.")

        return True
    except kfp_server_api.exceptions.ApiException as e:
        if e.status == 404:
            raise KubeflowPipelineException(
                f"Pipeline run *{run_id}* not found.")
        else:
            raise KubeflowPipelineException(
                f"Failed to validate pipeline run *{run_id}* (HTTP {e.status}: {e.reason})"
            )
    except Exception as e:
        raise KubeflowPipelineException(f"Failed to validate run *{run_id}*: {str(e)}") from e


def make_flow(
    obj,
    pipeline_name,
    production_token,
    tags,
    namespace,
    max_workers,
    kfp_client,
):
    # Attach @kubernetes.
    decorators._attach_decorators(obj.flow, [KubernetesDecorator.name])
    decorators._init(obj.flow)

    decorators._init_step_decorators(
        obj.flow, obj.graph, obj.environment, obj.flow_datastore, obj.logger
    )
    obj.graph = obj.flow._graph
    # Save the code package in the flow datastore so that both user code and
    # metaflow package can be retrieved during workflow execution.
    obj.package = MetaflowPackage(
        obj.flow,
        obj.environment,
        obj.echo,
        suffixes=obj.package_suffixes,
        flow_datastore=obj.flow_datastore if FEAT_ALWAYS_UPLOAD_CODE_PACKAGE else None,
    )

    # This blocks until the package is created
    if FEAT_ALWAYS_UPLOAD_CODE_PACKAGE:
        package_url = obj.package.package_url()
        package_sha = obj.package.package_sha()
    else:
        package_url, package_sha = obj.flow_datastore.save_data(
            [obj.package.blob], len_hint=1
        )[0]

    return KubeflowPipelines(
        kfp_client,
        pipeline_name,
        obj.graph,
        obj.flow,
        obj.package.package_metadata,
        package_sha,
        package_url,
        obj.metadata,
        obj.flow_datastore,
        obj.environment,
        obj.event_logger,
        obj.monitor,
        production_token,
        tags=tags,
        namespace=namespace,
        username=get_username(),
        max_workers=max_workers,
        description=obj.flow.__doc__,
    )
