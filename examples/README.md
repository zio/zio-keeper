# Examples


##Kubernetes example

In order to run kubernetes example you need to have configured `kubectl` and `helm` with k8 cluster. 

Below steps help you run example locally with [`kind`][Link-Kind]. 

```
sbt examples/docker:publishLocal
export ZIO_KEEPER_TAG=$(docker image ls zio-keeper-examples --format "{{.Tag}}" | head -1)

kind create cluster --name zio-keeper-experiment
kind --name zio-keeper-experiment load docker-image zio-keeper-examples:$ZIO_KEEPER_TAG

kubectl create namespace zio-keeper-experiment

helm install zio-keeper-node ./examples/chart --namespace zio-keeper-experiment --set image.tag=$ZIO_KEEPER_TAG,replicaCount=1
helm upgrade zio-keeper-node ./examples/chart --namespace zio-keeper-experiment --set image.tag=$ZIO_KEEPER_TAG,replicaCount=10
```


[Link-Kind]: https://kind.sigs.k8s.io 