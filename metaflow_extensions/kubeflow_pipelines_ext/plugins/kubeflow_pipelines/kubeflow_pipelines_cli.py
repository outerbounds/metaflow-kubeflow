import re
import base64
import tempfile
from kfp import Client
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


class IncorrectProductionToken(MetaflowException):
    headline = "Incorrect production token"


VALID_NAME = re.compile(r"[^a-zA-Z0-9_\-\.]")


def resolve_token(
    name, token_prefix, obj, authorize, given_token, generate_new_token, is_project
):
    # 1) retrieve the previous deployment, if one exists
    workflow = KubeflowPipelines.get_existing_deployment(name, obj.flow_datastore)
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
            obj.echo("    kfp create --authorize MY_TOKEN", fg="green")
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
        KubeflowPipelines.save_deployment_token(
            get_username(), name, token, obj.flow_datastore
        )
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
    obj.echo("    kfp create --authorize %s" % token, fg="green")
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
        if pipeline_name and VALID_NAME.fullmatch(pipeline_name):
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
        if name and VALID_NAME.fullmatch(name):
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
    yaml_only=False,
    max_workers=None,
):
    if not yaml_only and not url:
        raise KubeflowPipelineException("Please supply a Kubeflow Pipelines API Server URL with --url")

    kfp_client = Client(host=url) if url else None

    obj.echo("Compiling *%s* to Kubeflow Pipelines..." % obj.pipeline_name, bold=True)
    token = resolve_token(
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
        obj.echo("View at *{pipeline_url}*".format(pipeline_url=pipeline_url), bold=True)

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

    kfp_client = Client(host=url)

    params = {}
    for _, param in obj.flow._get_parameters():
        k = param.name.replace("-", "_").lower()
        if kwargs.get(k) is not None:
            params[param.name] = kwargs[k]

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
    obj.echo(
        "Fetching status for run *{kfp_run_id}* ...".format(kfp_run_id=kfp_run_id),
        bold=True,
    )

    kfp_client = Client(host=url)
    run_status = KubeflowPipelines.get_status(kfp_client, kfp_run_id)

    if run_status:
        obj.echo("Status: *{status}*".format(status=run_status))
    else:
        obj.echo("Run *{kfp_run_id}* not found.".format(kfp_run_id=kfp_run_id))


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
