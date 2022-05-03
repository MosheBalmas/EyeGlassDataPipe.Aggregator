set http_proxy=http://proxy-dmz.intel.com:912
set https_proxy=http://proxy-dmz.intel.com:912
:: IF NOT DEFINED %1 (set %1=false)
:: IF %1==login (az login)
az login

az account set --subscription d3e4d481-e08a-4d85-b01a-9ae56dab1e72
az aks get-credentials --resource-group laas-aiacdataeng-dev --name aiacdataengsadev-kubernetes_cluster
az acr login -n aiacdataengsadev

docker build -t aiacdataengsadev.azurecr.io/eyeglass.aggregator .
docker push aiacdataengsadev.azurecr.io/eyeglass.aggregator

kubectl create namespace eyeglass
kubectl apply -f kubernetes\deployment.yaml -n eyeglass