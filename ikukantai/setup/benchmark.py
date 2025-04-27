import logging
from logger_config.logger_config import setup_logger

from ikukantai.statemonitor.arch import MainMonitor, FunctionMonitor, PodMonitor
from ikukantai.statemonitor.api import StateAPI

from typing import List

from skippy.core.utils import parse_size_string

from sim import docker
from sim.benchmark import Benchmark
from sim.core import Environment
from sim.docker import ImageProperties
from sim.faas import FunctionDeployment, FunctionRequest, Function, FunctionImage, ScalingConfiguration, \
    DeploymentRanking, FunctionContainer, KubernetesResourceConfiguration


logger = logging.getLogger(__name__)
setup_logger()

class IkukantaiBenchmark(Benchmark):
    def setup(self, env: Environment):
        containers: docker.ContainerRegistry = env.container_registry

        # populate the global container registry with images       
        # TODO by ken: i dont know why image only work with arch x86
        containers.put(ImageProperties('app1', parse_size_string('56M'), arch='x86'))
        containers.put(ImageProperties('app2', parse_size_string('56M'), arch='x86'))
        containers.put(ImageProperties('app3', parse_size_string('56M'), arch='x86'))
        
        # log all the images in the container
        for name, tag_dict in containers.images.items():
            for tag, images in tag_dict.items():
                logger.debug(f"Prepare image for image with image name: {name}, tag: {tag}, image properties: {images}")

    def run(self, env: Environment):
        # deploy functions
        deployments = self.prepare_deployment()
        
        main_monitor = MainMonitor()
        logger.info("Finish adding MainMonitor to control deployments")

        for deployment in deployments:
            fn_monitor = FunctionMonitor(function=deployment.fn, mainmonitor=main_monitor)
            main_monitor.add_fnMonitor(fn_monitor) 
            yield from env.faas.deploy(deployment, main_monitor, fn_monitor)
                 
        # StateAPI.to_cold(main_monitor=main_monitor, function_name='app1', pod_name='1')         
                        
        # block until replicas become available (scheduling has finished and replicas have been deployed on the node)
        logger.info('waiting for replica')
        # TODO by LAZYken: adjust value for apps used
        yield env.process(env.faas.poll_available_replica('app1'))
        yield env.process(env.faas.poll_available_replica('app2'))
        yield env.process(env.faas.poll_available_replica('app3'))
        
        yield env.timeout(100)
        

        # # # run workload
        # ps = []
        # # execute 10 requests in parallel
        # logger.info('executing 2 app1 requests')
        # for i in range(2):
        #     ps.append(env.process(env.faas.invoke(FunctionRequest('app1'))))

        # # wait for invocation processes to finish
        # for p in ps:
        #     yield p

    def prepare_deployment(self):
        # TODO by LAZYken: adjust value for apps used
        # Design time
        app1_image = FunctionImage('app1')
        app2_image = FunctionImage('app2')
        app3_image = FunctionImage('app3')
        
        app1_fn = Function(name='app1', fn_images=[app1_image])
        app2_fn = Function(name='app2', fn_images=[app2_image])
        app3_fn = Function(name='app3', fn_images=[app3_image])
        
        # Run time
        
        # Default kubernetes requested resources
        
        # Custom defined requested resources
        app1_request = KubernetesResourceConfiguration.create_from_str(cpu='100m', memory='1024Mi')
        app2_request = KubernetesResourceConfiguration.create_from_str(cpu='100m', memory='1024Mi')
        app3_request = KubernetesResourceConfiguration.create_from_str(cpu='100m', memory='1024Mi')
        
        app1_container = FunctionContainer(fn_image=app1_image, resource_config=app1_request)
        app2_container = FunctionContainer(fn_image=app2_image, resource_config=app2_request)
        app3_container = FunctionContainer(fn_image=app3_image, resource_config=app3_request)
        
        app1_fd = FunctionDeployment(
            fn=app1_fn,
            fn_containers=[app1_container],
            scaling_config=ScalingConfiguration(),
            # deployment_ranking=DeploymentRanking()
        )
        
        app2_fd = FunctionDeployment(
            fn=app2_fn,
            fn_containers=[app2_container],
            scaling_config=ScalingConfiguration(),
            # deployment_ranking=DeploymentRanking()
        )
        
        app3_fd = FunctionDeployment(
            fn=app3_fn,
            fn_containers=[app3_container],
            scaling_config=ScalingConfiguration(),
            # deployment_ranking=DeploymentRanking()
        )
        
        return [app1_fd, app2_fd, app3_fd]


class SampleIkukantaiBenchmark(Benchmark):

    def setup(self, env: Environment):
        containers: docker.ContainerRegistry = env.container_registry

        # populate the global container registry with images
        containers.put(ImageProperties('python-pi-cpu', parse_size_string('58M'), arch='arm32'))
        containers.put(ImageProperties('python-pi-cpu', parse_size_string('58M'), arch='x86'))
        containers.put(ImageProperties('python-pi-cpu', parse_size_string('58M'), arch='aarch64'))

        containers.put(ImageProperties('resnet50-inference-cpu', parse_size_string('56M'), arch='arm32'))
        containers.put(ImageProperties('resnet50-inference-cpu', parse_size_string('56M'), arch='x86'))
        containers.put(ImageProperties('resnet50-inference-cpu', parse_size_string('56M'), arch='aarch64'))

        containers.put(ImageProperties('resnet50-inference-gpu', parse_size_string('56M'), arch='arm32'))
        containers.put(ImageProperties('resnet50-inference-gpu', parse_size_string('56M'), arch='x86'))
        containers.put(ImageProperties('resnet50-inference-gpu', parse_size_string('56M'), arch='aarch64'))

        # log all the images in the container
        for name, tag_dict in containers.images.items():
            for tag, images in tag_dict.items():
                logger.info('%s, %s, %s', name, tag, images)

    def run(self, env: Environment):
        # deploy functions
        deployments = self.prepare_deployments()

        for deployment in deployments:
            yield from env.faas.deploy(deployment)

        # block until replicas become available (scheduling has finished and replicas have been deployed on the node)
        logger.info('waiting for replica')
        yield env.process(env.faas.poll_available_replica('python-pi'))
        yield env.process(env.faas.poll_available_replica('resnet50-inference'))

        # # run workload
        ps = []
        # execute 10 requests in parallel
        logger.info('executing 2 python-pi requests')
        for i in range(2):
            ps.append(env.process(env.faas.invoke(FunctionRequest('python-pi'))))

        # logger.info('executing 10 resnet50-inference requests')
        # for i in range(10):
        #     ps.append(env.process(env.faas.invoke(FunctionRequest('resnet50-inference'))))

        # wait for invocation processes to finish
        for p in ps:
            yield p
            
        # generate profile
        # ia_generator = expovariate_arrival_profile(constant_rps_profile(rps=20))
        # ia_generator = constant_rps_profile(20)

        # run profile
        # yield from function_trigger(env, deployments[0], ia_generator, max_requests=10)
        # yield from function_trigger(env, deployments[1], ia_generator, max_requests=10)


    def prepare_deployments(self) -> List[FunctionDeployment]:
        resnet_fd = self.prepare_resnet_inference_deployment()

        python_pi_fd = self.prepare_python_pi_deployment()

        return [python_pi_fd, resnet_fd]

    def prepare_python_pi_deployment(self):
        # Design Time

        python_pi = 'python-pi'
        python_pi_cpu = FunctionImage(image='python-pi-cpu')
        python_pi_fn = Function(python_pi, fn_images=[python_pi_cpu])

        # Run time

        python_pi_fn_container = FunctionContainer(python_pi_cpu)

        python_pi_fd = FunctionDeployment(
            python_pi_fn,
            [python_pi_fn_container],
            ScalingConfiguration()
        )

        return python_pi_fd

    def prepare_resnet_inference_deployment(self):
        # Design time

        resnet_inference = 'resnet50-inference'
        inference_cpu = 'resnet50-inference-cpu'
        inference_gpu = 'resnet50-inference-gpu'

        resnet_inference_gpu = FunctionImage(image=inference_gpu)
        resnet_inference_cpu = FunctionImage(image=inference_cpu)
        resnet_fn = Function(resnet_inference, fn_images=[resnet_inference_gpu, resnet_inference_cpu])

        # Run time

        # default kubernetes requested resources
        resnet_cpu_container = FunctionContainer(resnet_inference_cpu)

        # custom defined requested resources
        request = KubernetesResourceConfiguration.create_from_str(cpu='100m', memory='1024Mi')
        resnet_gpu_container = FunctionContainer(resnet_inference_gpu, resource_config=request)

        resnet_fd = FunctionDeployment(
            resnet_fn,
            [resnet_cpu_container, resnet_gpu_container],
            ScalingConfiguration(),
            DeploymentRanking([inference_gpu, inference_cpu])
        )

        return resnet_fd