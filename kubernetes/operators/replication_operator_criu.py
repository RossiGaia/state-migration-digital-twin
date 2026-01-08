import kopf, requests, json, random, datetime, os
from kubernetes import client
from kubernetes.utils import create_from_dict
from k8s_utils import delete_from_dict
import os

@kopf.on.create("cyberphysicalapplications")
def create_fn(spec, name, namespace, meta, logger, **kwargs):
    k8s_client = client.ApiClient()
    k8s_custom_object = client.CustomObjectsApi()

    deployments = spec.get("deployments")
    preferred_affinity = spec.get("requirements").get("preferredAffinity")

    deployment_affinity = None
    deployment_configs = None
    deployment_namespace = None
    deployment_app_name = None
    deployment_prometheus_url = None

    if preferred_affinity == "" or preferred_affinity is None:
        deployment_configs = deployments[0].get("configs")
        deployment_affinity = deployments[0].get("affinity")

    else:
        for deployment in deployments:
            deployment_affinity = deployment.get("affinity")
            if deployment_affinity == preferred_affinity:
                deployment_configs = deployment.get("configs")
                break

    for config in deployment_configs:
        if config.get("kind") == "Deployment":
            config["spec"]["template"]["spec"].update(
                {"nodeSelector": {"zone": f"{deployment_affinity}"}}
            )
            deployment_namespace = config.get("metadata").get("namespace")
            deployment_app_name = config.get("metadata").get("labels").get("app")
            deployment_prometheus_url = (
                config.get("spec")
                .get("template")
                .get("metadata")
                .get("annotations")
                .get("prometheusUrl")
            )

        if config.get("kind") == "Service":
            kopf.label(config, {"debug": "current-service"})

        kopf.label(config, {"related-to": f"{name}"})
        kopf.adopt(config)
        try:
            create_from_dict(k8s_client, config)
        except Exception as e:
            logger.exception("Exception in object creation.")

    annotations_patch = {"metadata": {"annotations": dict(meta.annotations)}}
    annotations_patch["metadata"]["annotations"][
        "child-deployment-namespace"
    ] = deployment_namespace
    annotations_patch["metadata"]["annotations"][
        "child-deployment-app-name"
    ] = deployment_app_name
    annotations_patch["metadata"]["annotations"][
        "child-deployment-prometheus-url"
    ] = deployment_prometheus_url
    annotations_patch["metadata"]["annotations"][
        "child-deployment-affinity"
    ] = deployment_affinity

    group = "test.dev"
    version = "v1"
    plural = "cyberphysicalapplications"
    resp = k8s_custom_object.patch_namespaced_custom_object(
        group, version, namespace, plural, name, body=annotations_patch
    )


def get_prometheus_odte(prometheus_url, twin_of, logger):
    """Fetch ODTE value from Prometheus."""

    query_url = f'{prometheus_url}/api/v1/query?query=odte[pt="{twin_of}"]'.replace(
        "[", "{"
    ).replace("]", "}")
    logger.debug(query_url)

    odte = None
    try:
        resp = requests.get(query_url)
        odte = float(json.loads(resp.text)["data"]["result"][0]["value"][1])
        return odte

    except Exception as e:
        logger.warn(f"Prometheus not available. {e}")
        raise Exception


def choose_next_deployment(deployments, current_deployment_affinity):
    """Select a new deployment avoiding the current one."""

    available_deployments = [
        d for d in deployments if d.get("affinity") != current_deployment_affinity
    ]
    return random.choice(available_deployments) if available_deployments else None


def ensure_pods_ready(k8s_core_v1, app_name, namespace, logger):
    """Wait until all pods of the deployment are ready."""

    label_selector = f"app={app_name}"
    try:
        resp = k8s_core_v1.list_namespaced_pod(namespace, label_selector=label_selector)
    except:
        logger.error("Cannot list pods.")
        return

    pods = resp.items

    for pod in pods:
        pod_ready = False
        pod_name = pod.metadata.name

        while not pod_ready:
            try:
                pod_status = k8s_core_v1.read_namespaced_pod_status(pod_name, namespace)
            except:
                logger.error("Cannot read pod status.")

            conditions = pod_status.status.conditions
            for condition in conditions:
                if condition.type == "Ready" and condition.status == "True":
                    pod_ready = True


def ensure_pod_termination(k8s_core_v1, app_name, namespace, logger):
    terminated = False
    label_selector = f"app={app_name}"
    while not terminated:
        try:
            resp = k8s_core_v1.list_namespaced_pod(
                namespace, label_selector=label_selector
            )
            if len(resp.items) == 0:
                terminated = True
        except Exception as e:
            logger.info(f"Cannot list pods. {e}")


@kopf.on.update("cyberphysicalapplications")
def update_fn(name, spec, namespace, **kwargs):

    k8s_client_custom_object = client.CustomObjectsApi()
    migrate_field = spec.get("migrate", None)
    if migrate_field:
        cpa_patch = {"spec": {"migrate": migrate_field}}
        group = "test.dev"
        version = "v1"
        plural = "cyberphysicalapplications"
        obj = k8s_client_custom_object.patch_namespaced_custom_object(
            group, version, namespace, plural, name, body=cpa_patch
        )


@kopf.on.field("cyberphysicalapplications", field="spec.migrate", old=False, new=True)
def migrate_fn(spec, namespace, meta, name, old, new, logger, **_):

    k8s_client = client.ApiClient()
    k8s_core_v1 = client.CoreV1Api()
    k8s_custom_object = client.CustomObjectsApi()

    # trigger a migration
    timestamps = []

    # get vars
    deployments = spec.get("deployments")
    current_deployment_affinity = meta.get("annotations").get(
        "child-deployment-affinity"
    )
    current_deployment_namespace = meta.get("annotations").get(
        "child-deployment-namespace"
    )
    current_deployment_app_name = meta.get("annotations").get(
        "child-deployment-app-name"
    )
    next_deployment = choose_next_deployment(
        deployments, current_deployment_affinity
    )
    next_deployment_configs = next_deployment.get("configs")
    next_deployment_affinity = next_deployment.get("affinity")


    # call checkpoint api on the node where the pod is currently running
    operation_name = "Checkpoint old instance"
    operation_start_time = datetime.datetime.now()

    # get the node where the pod is running
    label_selector = f"app={current_deployment_app_name}"
    resp = k8s_core_v1.list_namespaced_pod(
            namespace, label_selector=label_selector
        )

    if len(resp.items) <= 0:
        logger.error(f"No namespaced pods found. Unable to get pod name, container name and host ip.")
        return
    
    # node_name = resp.items[0].spec.node_name
    current_pod_name = resp.items[0].metadata.name
    current_container_name = resp.items[0].spec.containers[0].name
    host_ip = resp.items[0].status.host_ip


    # call the checkpoint api
    checkpoint_url = f"http://{host_ip}:9000/checkpoint"
    namespace = current_deployment_namespace       
    pod_name = current_pod_name
    container_name = current_container_name
    token = os.environ.get("KUBELET_ACCESS_TOKEN", None)
    if not token:
        logger.error(f"Token for kubelet access is not set.")
        return

    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "namespace": namespace,
        "pod": pod_name,
        "container": container_name,
        "token": token
    }

    resp = requests.post(checkpoint_url, headers=headers, data=json.dumps(data))
    print(resp.text)

    if resp.status_code == 500:
        logger.error(f"Error in the checkpoint call.")
        return

    new_image_name = f"rssgai/checkpoint-image:{pod_name}_{container_name}"
    print(new_image_name)

    operation_end_time = datetime.datetime.now()
    timestamps.append([operation_name, operation_start_time, operation_end_time])


    # stop the currently running pod
    operation_name = "Deleting old instance"
    operation_start_time = operation_end_time
    for depl in deployments:
        if depl.get("affinity") == current_deployment_affinity:
            for config in depl.get("configs"):
                delete_from_dict(k8s_client, config)

    ensure_pod_termination(k8s_core_v1, current_deployment_app_name, namespace, logger)
    operation_end_time = datetime.datetime.now()
    timestamps.append([operation_name, operation_start_time, operation_end_time])
    

    # start the new pod with the restored image (override the image from confs)
    operation_name = "Start new instance"
    operation_start_time = operation_end_time
    annotations_patch = {"metadata": {"annotations": dict(meta.annotations)}}
    for config in next_deployment_configs:
        if config.get("kind") == "Deployment":
            config["spec"]["template"]["spec"].update(
                {"nodeSelector": {"zone": f"{next_deployment_affinity}"}}
            )

            # image ovveride
            config["spec"]["template"]["spec"]["containers"][0]["image"] = new_image_name
            # config["spec"]["template"]["spec"]["containers"][0].pop("command", None)

            next_deployment_namespace = config.get("metadata").get("namespace")
            next_deployment_app_name = (
                config.get("metadata").get("labels").get("app")
            )
            annotations_patch["metadata"]["annotations"][
                "child-deployment-namespace"
            ] = next_deployment_namespace
            annotations_patch["metadata"]["annotations"][
                "child-deployment-app-name"
            ] = (config.get("metadata").get("labels").get("app"))
            annotations_patch["metadata"]["annotations"][
                "child-deployment-prometheus-url"
            ] = (
                config.get("spec")
                .get("template")
                .get("metadata")
                .get("annotations")
                .get("prometheusUrl")
            )
            annotations_patch["metadata"]["annotations"][
                "child-deployment-affinity"
            ] = next_deployment_affinity


        if config.get("kind") == "Service":
            next_deployment_service_name = config.get("metadata").get("name")
            next_deployment_service = config

        kopf.adopt(config)
        kopf.label(config, {"related-to": f"{name}"})
        try:
            create_from_dict(k8s_client, config)
        except:
            logger.exception("Exception creating new object.")

    group = "test.dev"
    version = "v1"
    plural = "cyberphysicalapplications"
    resp = k8s_custom_object.patch_namespaced_custom_object(
        group, version, namespace, plural, name, body=annotations_patch
    )

    # wait for it to start correctly
    ensure_pods_ready(
        k8s_core_v1, next_deployment_app_name, next_deployment_namespace, logger
    )
    logger.info("Deployment's pods started.")

    kopf.label(next_deployment_service, {"debug": "current-service"})
    resp = k8s_core_v1.patch_namespaced_service(
        next_deployment_service_name,
        next_deployment_namespace,
        next_deployment_service,
    )

    operation_end_time = datetime.datetime.now()
    timestamps.append([operation_name, operation_start_time, operation_end_time])

    return