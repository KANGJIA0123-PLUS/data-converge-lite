# RBAC 升级核对

本次 Chart 将 `{{ .Chart.Name }}-cluster-role` / `{{ .Chart.Name }}-cluster-role-binding` 改为命名空间内的 `{{ .Chart.Name }}-role` / `{{ .Chart.Name }}-role-binding`。新 Role 仅授予核心 API 组中 Secret、ConfigMap 的 `get`、`create`、`delete`。

## 命名空间与完整发布包

- Helm 的发布命名空间（`.Release.Namespace`）、`global.current_namespace`、服务账号所在命名空间必须一致。仓库中的安装和卸载 Job 未显式设置 `metadata.namespace`，会使用发布命名空间；新 Role、RoleBinding 及绑定的服务账号则使用 `global.current_namespace`。
- 参数中的 `chart_name` 是完整发布包 `Chart.yaml` 的 `name`，不是 Helm release 名称。以下占位值须替换为实际发布参数。
- 本仓库不含完整的 `Chart.yaml`、values、ServiceAccount 模板，也不含 Job 调用的 `pre-install.sh`、`pre-delete.sh`、`post-delete.sh` 和 `/var/scripts/sudo_init.sh`。完整镜像和 Chart 依赖外部 BuildTool，无法仅凭仓库确认这些脚本是否还需要跨命名空间、证书资源或其他操作权限。
- 应先核对完整发布包的脚本和渲染清单，再在验证环境覆盖首次安装、升级、启动及卸载。按实际调用补齐必要权限；不要因缺少脚本就恢复通配 API 组或集群级授权。

## 旧集群权限不会自动移除

旧资源使用 `pre-install,pre-upgrade` hook 和 `before-hook-creation`。新资源的名称与类型都已改变，创建新 hook 不会删除旧 ClusterRole、ClusterRoleBinding；Helm 也不把这些 hook 资源作为普通发布资源管理。因此，单次升级成功不能证明集群权限已经收敛。参见 [Helm hook 生命周期与清理策略](https://helm.sh/docs/topics/charts_hooks/#hook-resources-are-not-managed-with-corresponding-releases)。

管理员先执行以下只读核对，检查旧绑定的 `subjects`、`roleRef`、归属信息及所有引用。旧名称只包含 Chart 名称，可能涉及其他发布或命名空间，不能只凭名称判断可删除。

```bash
chart_name='<Chart.yaml 中的 name>'
namespace='<发布命名空间>'

kubectl get clusterrole "${chart_name}-cluster-role" -o yaml
kubectl get clusterrolebinding "${chart_name}-cluster-role-binding" -o yaml
kubectl get clusterrolebindings -o wide
kubectl get rolebindings --all-namespaces -o wide
kubectl -n "$namespace" get serviceaccount "${chart_name}-sa"
kubectl -n "$namespace" get role "${chart_name}-role" -o yaml
kubectl -n "$namespace" get rolebinding "${chart_name}-role-binding" -o yaml
```

清理前保留旧 RBAC 清单，并确认新 Role、RoleBinding 已存在、绑定正确且完整发布包已通过权限验证。管理员确认归属和影响后，先删除**仅属于本次迁移**的 `${chart_name}-cluster-role-binding`，再核对所有 ClusterRoleBinding 和 RoleBinding；只有不存在任何对旧 ClusterRole 的引用时，才删除 `${chart_name}-cluster-role`。若资源被共享，应先协调对应发布迁移，不直接删除。不要按通配符批量清理，也不要给应用服务账号增加删除集群 RBAC 的权限。

## 部署前权限验证

在验证环境中先解除旧绑定及其他非预期授权，再做以下检查。旧 ClusterRoleBinding 仍然存在时，`can-i` 会把旧权限合并进去，结果不能证明新 Role 足够或权限已收敛；Kubernetes RBAC 的授权规则是累加的。参见 [Kubernetes RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)。

下面命令适用于 Bash；执行者需有对应用户和组的 impersonate 权限。模拟身份包含服务账号默认组，避免遗漏组授权。

```bash
rbac_subject="system:serviceaccount:${namespace}:${chart_name}-sa"
rbac_args=(
  "--as=${rbac_subject}"
  "--as-group=system:serviceaccounts"
  "--as-group=system:serviceaccounts:${namespace}"
  "--as-group=system:authenticated"
)

for resource in secrets configmaps; do
  for verb in get create delete; do
    kubectl auth can-i "$verb" "$resource" -n "$namespace" "${rbac_args[@]}"
  done
done

for resource in secrets configmaps; do
  for verb in list watch update patch; do
    kubectl auth can-i "$verb" "$resource" -n "$namespace" "${rbac_args[@]}"
  done
done

other_namespace='<另一个实际命名空间>'
kubectl auth can-i get secrets -n "$other_namespace" "${rbac_args[@]}"
kubectl auth can-i get configmaps -n "$other_namespace" "${rbac_args[@]}"
```

第一组应返回 `yes`；仅由本次 Role 授权时，后两组应返回 `no`。若存在经过批准的其他授权，逐项解释其来源；非预期的 `yes` 需要继续核对旧绑定和组授权。`can-i` 只验证授权，不会执行安装脚本，也不能代替完整发布包的生命周期验证。外部脚本若访问证书资源，应先确认实际 API 组、资源名、作用域和操作，再验证对应权限。

本地仅能检查模板内容及替换参数后的 YAML；没有完整发布包和验证集群时，不应宣称已完成 Helm 安装或权限兼容验收。新 Role、RoleBinding 也保留了 hook 生命周期，卸载后的清理由发布流程在所有卸载 Job 完成后核对处理。
